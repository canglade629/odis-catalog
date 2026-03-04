"""Log capture handler for storing pipeline logs in PostgreSQL."""
import logging
import threading
import time
from datetime import datetime, timedelta
from queue import Queue, Empty
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings


def _get_sync_engine():
    """Lazy sync engine for log writes (used from background thread)."""
    settings = get_settings()
    url = settings.sync_database_url
    return create_engine(url, pool_pre_ping=True, pool_size=2)


_engine = None


def _engine_for_logs():
    global _engine
    if _engine is None:
        _engine = _get_sync_engine()
    return _engine


class PostgresLogHandler(logging.Handler):
    """Custom logging handler that writes logs to PostgreSQL job_logs with batching."""

    def __init__(
        self,
        job_id: str,
        task_id: Optional[str] = None,
        batch_size: int = 10,
        flush_interval: float = 2.0,
    ):
        super().__init__()
        self.job_id = job_id
        self.task_id = task_id
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_queue: Queue = Queue()
        self.is_running = True
        self.flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self.flush_thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            log_entry = {
                "timestamp": datetime.utcnow(),
                "level": record.levelname,
                "message": message,
                "logger_name": record.name,
                "job_id": self.job_id,
                "task_id": self.task_id,
            }
            self.log_queue.put(log_entry)
        except Exception:
            self.handleError(record)

    def _flush_worker(self) -> None:
        batch: List[Dict[str, Any]] = []
        last_flush = time.time()
        while self.is_running:
            try:
                try:
                    log_entry = self.log_queue.get(timeout=0.5)
                    batch.append(log_entry)
                except Empty:
                    pass
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size
                    or (batch and current_time - last_flush >= self.flush_interval)
                )
                if should_flush:
                    self._write_batch(batch)
                    batch = []
                    last_flush = current_time
            except Exception as e:
                print(f"Error in log flush worker: {e}")
        if batch:
            self._write_batch(batch)

    def _write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return
        try:
            engine = _engine_for_logs()
            SessionLocal = sessionmaker(bind=engine, autoflush=False)
            with SessionLocal() as session:
                for entry in batch:
                    ts = entry.get("timestamp") or datetime.utcnow()
                    session.execute(
                        text(
                            """
                            INSERT INTO job_logs (job_id, timestamp, level, message, logger_name, task_id)
                            VALUES (:job_id, :ts, :level, :message, :logger_name, :task_id)
                            """
                        ),
                        {
                            "job_id": self.job_id,
                            "ts": ts,
                            "level": entry.get("level"),
                            "message": entry.get("message"),
                            "logger_name": entry.get("logger_name"),
                            "task_id": entry.get("task_id"),
                        },
                    )
                session.commit()
        except Exception as e:
            print(f"Failed to write logs to PostgreSQL: {e}")

    def flush(self) -> None:
        time.sleep(self.flush_interval + 0.5)

    def close(self) -> None:
        self.is_running = False
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5.0)
        super().close()


# Legacy alias (logging is PostgreSQL-backed)
FirestoreLogHandler = PostgresLogHandler


class LogCaptureContext:
    """Context manager for capturing logs during pipeline execution."""

    def __init__(
        self,
        job_id: str,
        task_id: Optional[str] = None,
        logger_name: str = "app",
    ):
        self.job_id = job_id
        self.task_id = task_id
        self.logger_name = logger_name
        self.handler: Optional[PostgresLogHandler] = None
        self.logger: Optional[logging.Logger] = None

    def __enter__(self) -> "LogCaptureContext":
        self.handler = PostgresLogHandler(self.job_id, self.task_id)
        self.handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        self.logger = logging.getLogger(self.logger_name)
        self.logger.addHandler(self.handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.handler and self.logger:
            self.logger.removeHandler(self.handler)
            self.handler.close()
        return False


def cleanup_old_logs(days: int = 30) -> int:
    """Delete job_logs older than specified days. Returns number deleted."""
    engine = _engine_for_logs()
    cutoff = datetime.utcnow() - timedelta(days=days)
    with engine.connect() as conn:
        r = conn.execute(
            text("DELETE FROM job_logs WHERE timestamp < :cutoff"),
            {"cutoff": cutoff},
        )
        conn.commit()
        return r.rowcount or 0
