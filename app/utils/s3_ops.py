"""S3 operations utilities (Scaleway-compatible)."""
import io
import logging
from typing import List, BinaryIO, Optional

import boto3
from botocore.config import Config as BotoConfig

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _parse_s3_path(path: str) -> tuple[str, str]:
    """Return (bucket, key) from s3://bucket/key or bucket/key."""
    path = path.strip()
    if path.startswith("s3://"):
        path = path[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


class S3Operations:
    """Helper class for S3 operations (Scaleway)."""

    def __init__(self):
        """Initialize S3 client with Scaleway endpoint."""
        self.settings = get_settings()
        self.bucket_name = self.settings.scw_bucket_name
        self._client = boto3.client(
            "s3",
            endpoint_url=self.settings.scw_object_storage_endpoint,
            region_name=self.settings.scw_region,
            aws_access_key_id=self.settings.scw_access_key,
            aws_secret_access_key=self.settings.scw_secret_key,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )

    def _full_path(self, key: str) -> str:
        """Build full s3:// path for a key."""
        return f"s3://{self.bucket_name}/{key}"

    def list_files(self, prefix: str) -> List[str]:
        """
        List files in an S3 path.

        Args:
            prefix: Key prefix (e.g. 'raw/' or 'raw/accueillants/')

        Returns:
            List of full s3:// paths
        """
        if prefix.startswith("s3://"):
            _, prefix = _parse_s3_path(prefix)
        elif prefix.startswith(f"{self.bucket_name}/"):
            prefix = prefix[len(self.bucket_name) + 1 :]
        files = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents") or []:
                key = obj["Key"]
                if not key.endswith("/"):
                    files.append(self._full_path(key))
        return files

    def download_file(self, s3_path: str) -> bytes:
        """
        Download a file from S3.

        Args:
            s3_path: Full S3 path (s3://bucket/key)

        Returns:
            File contents as bytes
        """
        bucket, key = _parse_s3_path(s3_path)
        if not bucket:
            bucket = self.bucket_name
        resp = self._client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def download_to_stream(self, s3_path: str) -> io.BytesIO:
        """
        Download a file to a BytesIO stream.

        Args:
            s3_path: Full S3 path

        Returns:
            BytesIO stream with file contents
        """
        content = self.download_file(s3_path)
        return io.BytesIO(content)

    def upload_file(self, local_file: BinaryIO, s3_path: str) -> str:
        """
        Upload a file to S3.

        Args:
            local_file: File object to upload
            s3_path: Destination S3 path (s3://bucket/key)

        Returns:
            Full S3 path of uploaded file
        """
        bucket, key = _parse_s3_path(s3_path)
        if not bucket:
            bucket = self.bucket_name
        local_file.seek(0)
        self._client.upload_fileobj(local_file, Bucket=bucket, Key=key)
        logger.info("Uploaded file to %s", s3_path)
        return s3_path if s3_path.startswith("s3://") else self._full_path(key)

    def upload_from_string(self, content: str, s3_path: str) -> str:
        """
        Upload string content to S3.

        Args:
            content: String content to upload
            s3_path: Destination S3 path

        Returns:
            Full S3 path of uploaded file
        """
        bucket, key = _parse_s3_path(s3_path)
        if not bucket:
            bucket = self.bucket_name
        self._client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        logger.info("Uploaded content to %s", s3_path)
        return s3_path if s3_path.startswith("s3://") else self._full_path(key)

    def file_exists(self, s3_path: str) -> bool:
        """
        Check if a file exists in S3.

        Args:
            s3_path: Full S3 path

        Returns:
            True if file exists, False otherwise
        """
        bucket, key = _parse_s3_path(s3_path)
        if not bucket:
            bucket = self.bucket_name
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def get_file_info(self, s3_path: str) -> dict:
        """
        Get file metadata.

        Args:
            s3_path: Full S3 path

        Returns:
            Dictionary with name, size, content_type, last_modified
        """
        bucket, key = _parse_s3_path(s3_path)
        if not bucket:
            bucket = self.bucket_name
        resp = self._client.head_object(Bucket=bucket, Key=key)
        return {
            "name": key.split("/")[-1],
            "size": resp.get("ContentLength", 0),
            "content_type": resp.get("ContentType"),
            "updated": resp.get("LastModified"),
        }


_s3_ops: Optional[S3Operations] = None


def get_s3_operations() -> S3Operations:
    """Get or create global S3 operations instance."""
    global _s3_ops
    if _s3_ops is None:
        _s3_ops = S3Operations()
    return _s3_ops
