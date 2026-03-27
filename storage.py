"""
S3 storage layer for raw filing exhibit text.
Raw text is stored in S3 so parsed data can be regenerated without re-fetching from EDGAR.
"""

import hashlib
import os

import boto3
from botocore.exceptions import ClientError


def _get_s3_config() -> dict:
    """Read S3 credentials from Streamlit secrets or env vars."""
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            return {
                "bucket": st.secrets.get("S3_BUCKET", ""),
                "aws_access_key_id": st.secrets.get("AWS_ACCESS_KEY_ID", ""),
                "aws_secret_access_key": st.secrets.get("AWS_SECRET_ACCESS_KEY", ""),
                "region": st.secrets.get("AWS_REGION", "us-east-1"),
            }
    except Exception:
        pass
    return {
        "bucket": os.environ.get("S3_BUCKET", ""),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "region": os.environ.get("AWS_REGION", "us-east-1"),
    }


_client = None


def _get_client():
    """Return a cached boto3 S3 client."""
    global _client
    if _client is None:
        cfg = _get_s3_config()
        _client = boto3.client(
            "s3",
            aws_access_key_id=cfg["aws_access_key_id"],
            aws_secret_access_key=cfg["aws_secret_access_key"],
            region_name=cfg["region"],
        )
    return _client


def _get_bucket() -> str:
    cfg = _get_s3_config()
    bucket = cfg["bucket"]
    if not bucket:
        raise RuntimeError("Missing S3_BUCKET. Set it in .streamlit/secrets.toml or as an env var.")
    return bucket


def content_hash(text: str) -> str:
    """Return a short MD5 hash of the text content."""
    return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()[:12]


def make_s3_key(ticker: str, filing_date: str, text: str) -> str:
    """Build the S3 object key for a filing's raw text."""
    h = content_hash(text)
    return f"filings/{ticker}/{filing_date}_{h}.txt"


def upload_raw_text(ticker: str, filing_date: str, text: str) -> str:
    """Upload raw exhibit text to S3. Returns the S3 key."""
    key = make_s3_key(ticker, filing_date, text)
    bucket = _get_bucket()
    _get_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
    )
    return key


def download_raw_text(s3_key: str) -> str:
    """Download raw exhibit text from S3."""
    bucket = _get_bucket()
    resp = _get_client().get_object(Bucket=bucket, Key=s3_key)
    return resp["Body"].read().decode("utf-8")


def raw_text_exists(s3_key: str) -> bool:
    """Check if a raw text file exists in S3."""
    bucket = _get_bucket()
    try:
        _get_client().head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError:
        return False
