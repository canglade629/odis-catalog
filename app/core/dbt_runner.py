"""
Run DBT (dbt run) from the app. DBT project may live in-repo (dbt/) or externally.
Bronze path is passed via DBT_S3_PATH or derived from app S3 settings.
"""
import asyncio
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def get_dbt_project_dir() -> Path:
    """Return the absolute path to the dbt project (repo root / dbt)."""
    # Assume app is at app/core/dbt_runner.py, so repo root is parent of app
    app_dir = Path(__file__).resolve().parent.parent.parent
    return app_dir / "dbt"


def run_dbt(
    silver_only: bool = True,
    run_tests: bool = False,
    timeout_seconds: Optional[int] = 1800,
) -> Tuple[bool, str, int]:
    """
    Run dbt (dbt run, optionally dbt test). Intended to be called from a thread pool.

    Args:
        silver_only: If True, run only silver (and staging). If False, run all models.
        run_tests: If True, also run dbt test after dbt run.
        timeout_seconds: Max time for the whole run (default 30 min). None = no timeout.

    Returns:
        (success, message, exit_code)
    """
    dbt_dir = get_dbt_project_dir()
    if not dbt_dir.is_dir():
        return False, f"DBT project not found at {dbt_dir}", -1

    settings = get_settings()
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = env.get("DBT_DUCKDB_PATH", "target/odace.duckdb")
    # S3 bronze path for DBT sources (Scaleway)
    env["DBT_S3_PATH"] = env.get("DBT_S3_PATH", f"{settings.s3_bucket_url}/bronze")
    env["DBT_GCS_DELTA_PATH"] = env["DBT_S3_PATH"]  # backward compat for dbt profile

    select = "staging silver" if silver_only else "staging silver gold"
    cmd_run = ["dbt", "run", "--select", select]

    try:
        result = subprocess.run(
            cmd_run,
            cwd=str(dbt_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        out = (result.stdout or "") + (result.stderr or "")
        if result.returncode != 0:
            return False, out or f"dbt run exited with {result.returncode}", result.returncode

        if run_tests:
            result_test = subprocess.run(
                ["dbt", "test", "--select", select],
                cwd=str(dbt_dir),
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            out += (result_test.stdout or "") + (result_test.stderr or "")
            if result_test.returncode != 0:
                return (
                    False,
                    out or f"dbt test exited with {result_test.returncode}",
                    result_test.returncode,
                )

        return True, out or "dbt run completed", 0
    except subprocess.TimeoutExpired:
        return False, "dbt run timed out", -1
    except FileNotFoundError:
        return False, "dbt CLI not found (install dbt-core and dbt-duckdb)", -1
    except Exception as e:
        logger.exception("dbt run failed")
        return False, str(e), -1


async def run_dbt_async(
    silver_only: bool = True,
    run_tests: bool = False,
    timeout_seconds: Optional[int] = 1800,
) -> Tuple[bool, str, int]:
    """Async wrapper: run dbt in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: run_dbt(silver_only=silver_only, run_tests=run_tests, timeout_seconds=timeout_seconds),
    )
