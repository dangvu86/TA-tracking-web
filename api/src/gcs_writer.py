"""Google Cloud Storage writer for TA analysis results.

Uploads to a public bucket:
  - analysis/{YYYY-MM-DD}.json   (historical archive, long cache)
  - latest.json                  (pointer to most recent, short cache)
  - history/...                  (price history for in-browser intraday calc)

Uses Application Default Credentials (automatic on Cloud Run).
"""

import gzip
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from google.cloud import storage
    _GCS_READY = True
except ImportError as e:
    _GCS_READY = False
    logger.warning(f"google-cloud-storage not installed: {e}")


def upload_analysis(
    bucket_name: str,
    date_str: str,
    data: dict,
    update_latest: bool = True,
) -> bool:
    """Upload analysis result to GCS archive (+ optionally latest pointer).

    Args:
        bucket_name: GCS bucket name (e.g., "ta-tracking-data")
        date_str: Date string in YYYY-MM-DD format
        data: Analysis payload (dict matching AnalyzeResponse schema)
        update_latest: If True (default) also overwrites latest.json. Set False
            when backfilling a historical date so today's pointer isn't clobbered.

    Returns:
        True on success, False on failure.
    """
    if not _GCS_READY:
        logger.error("GCS library unavailable; cannot upload")
        return False

    try:
        payload = json.dumps(data, ensure_ascii=False)
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        archive = bucket.blob(f"analysis/{date_str}.json")
        archive.cache_control = "public, max-age=3600"
        archive.upload_from_string(payload, content_type="application/json; charset=utf-8")
        logger.info(f"Uploaded gs://{bucket_name}/analysis/{date_str}.json ({len(payload):,} bytes)")

        if update_latest:
            latest = bucket.blob("latest.json")
            latest.cache_control = "public, max-age=300"
            latest.upload_from_string(payload, content_type="application/json; charset=utf-8")
            logger.info(f"Updated gs://{bucket_name}/latest.json")

        return True
    except Exception as e:
        logger.error(f"GCS upload failed: {e}", exc_info=True)
        return False


def get_analysis(bucket_name: str, date_str: Optional[str] = None) -> Optional[dict]:
    """Read analysis from GCS. If date_str is None, reads latest.json."""
    if not _GCS_READY:
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob_name = f"analysis/{date_str}.json" if date_str else "latest.json"
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())
    except Exception as e:
        logger.warning(f"GCS read failed: {e}")
        return None


def upload_history(bucket_name: str, path: str, payload: dict, cache_control: str) -> bool:
    """Upload a price-history file gzip-encoded (about 1.2MB raw, ~400KB gzipped)."""
    if not _GCS_READY:
        logger.error("GCS library unavailable; cannot upload")
        return False

    try:
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(path)
        blob.cache_control = cache_control
        blob.content_encoding = "gzip"
        data = gzip.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        blob.upload_from_string(data, content_type="application/json; charset=utf-8")
        logger.info(f"Uploaded gs://{bucket_name}/{path} ({len(data):,} bytes gzipped)")
        return True
    except Exception as e:
        logger.error(f"History upload failed: {e}", exc_info=True)
        return False
