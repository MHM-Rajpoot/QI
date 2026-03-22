"""Background admin job management and task runners."""

import contextlib
import io
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from flask import Flask

from db.snowflake import get_db
from scripts.fetch_data import fetch_and_save_data
from services.programme_plans_service import ProgrammePlansService


def _utc_timestamp() -> str:
    """Return an ISO8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


class BackgroundJobManager:
    """Simple in-memory background job manager for admin operations."""

    def __init__(self, *, max_workers: int = 2):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="admin-job",
        )
        self._jobs: Dict[str, Dict[str, object]] = {}
        self._lock = threading.Lock()

    def submit(
        self,
        name: str,
        func: Callable[[], Dict[str, object]],
        *,
        app: Optional[Flask] = None,
    ) -> Dict[str, object]:
        """Submit a background job and return its initial state."""
        job_id = str(uuid.uuid4())
        job: Dict[str, object] = {
            "id": job_id,
            "name": name,
            "status": "queued",
            "message": f"{name} queued",
            "result": None,
            "error": None,
            "created_at": _utc_timestamp(),
            "started_at": None,
            "finished_at": None,
        }

        with self._lock:
            self._jobs[job_id] = job

        self._executor.submit(self._run_job, job_id, func, app)
        return dict(job)

    @staticmethod
    def _execute_job(func: Callable[[], Dict[str, object]], app: Optional[Flask] = None) -> Dict[str, object]:
        """Run a job inside a Flask app context when available."""
        if app is None:
            return func()
        with app.app_context():
            return func()

    def _run_job(
        self,
        job_id: str,
        func: Callable[[], Dict[str, object]],
        app: Optional[Flask] = None,
    ):
        self._update_job(
            job_id,
            status="running",
            message="Job is running",
            started_at=_utc_timestamp(),
        )

        try:
            result = self._execute_job(func, app) or {}
            message = result.get("message") if isinstance(result, dict) else None
            self._update_job(
                job_id,
                status="completed",
                message=message or "Job completed successfully",
                result=result,
                finished_at=_utc_timestamp(),
            )
        except Exception as exc:
            self._update_job(
                job_id,
                status="failed",
                message=str(exc) or "Job failed",
                error={
                    "message": str(exc) or "Job failed",
                },
                finished_at=_utc_timestamp(),
            )

    def _update_job(self, job_id: str, **changes):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.update(changes)

    def get_job(self, job_id: str) -> Optional[Dict[str, object]]:
        """Return a serializable snapshot of a job."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return dict(job)


def refresh_programme_plans_job(*, config_file: str, csv_file: str) -> Dict[str, object]:
    """Refresh the Programme Plans snapshot CSV."""
    service = ProgrammePlansService(config_file=config_file, snapshot_csv=csv_file)
    df = service.refresh_snapshot()
    return {
        "message": "Programme Plans data refreshed from current Snowflake data",
        "row_count": int(len(df)),
        "dataset": "Programme Plans Dataset",
    }


def refresh_all_data_job(
    *,
    config_file: str,
    root_path: str,
    data_dir: str,
    programme_plans_csv: str,
) -> Dict[str, object]:
    """Refresh all Snowflake-backed datasets."""
    os.makedirs(data_dir, exist_ok=True)

    output_parts = []
    refreshed_files = []

    enrolment_log = io.StringIO()
    with contextlib.redirect_stdout(enrolment_log):
        fetch_and_save_data(config_file, output_dir=data_dir)
    if enrolment_log.getvalue().strip():
        output_parts.append(enrolment_log.getvalue().strip())
    refreshed_files.extend([
        "enrolment_total.csv",
        "enrolment_by_ssa.csv",
        "enrolment_by_provider.csv",
        "enrolment_by_age.csv",
        "enrolment_by_level.csv",
    ])

    programme_plans_service = ProgrammePlansService(config_file=config_file, snapshot_csv=programme_plans_csv)
    programme_plans_df = programme_plans_service.refresh_snapshot()
    output_parts.append(f"[Programme Plans] Refreshed {len(programme_plans_df)} rows")
    refreshed_files.append("programme_plans_dataset")

    metadata_csv = os.path.join(data_dir, "snowflake_columns_presentation_staging_ilr.csv")
    metadata_query = """
    SELECT
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        t.TABLE_TYPE,
        c.COLUMN_NAME,
        c.ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.COLUMNS c
    JOIN INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
       AND c.TABLE_NAME = t.TABLE_NAME
    WHERE c.TABLE_SCHEMA IN ('PRESENTATION', 'STAGING_ILR')
    ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """

    db = get_db(config_file)
    try:
        metadata_df = db.execute_query(metadata_query)
    finally:
        db.disconnect()

    metadata_df.to_csv(metadata_csv, index=False)
    output_parts.append(f"[Metadata] Refreshed {len(metadata_df)} rows")
    refreshed_files.append("metadata_dataset")

    refreshed_files = sorted(set(refreshed_files))
    return {
        "message": f"Data refreshed successfully. Updated {len(refreshed_files)} resources.",
        "output": "\n".join(part for part in output_parts if part),
        "refreshed_files": refreshed_files,
        "root_path": root_path,
    }


def train_models_job(*, project_root: str) -> Dict[str, object]:
    """Train forecasting models and refresh saved outputs."""
    training_data = os.path.join(project_root, "data", "enrolment_total.csv")
    output_dir = os.path.join(project_root, "saved_models")

    if not os.path.exists(training_data):
        raise RuntimeError("Training data is not available. Refresh data before training models.")

    from scripts.train_local import train_all_models_from_local

    training_log = io.StringIO()
    with contextlib.redirect_stdout(training_log), contextlib.redirect_stderr(training_log):
        training_summary = train_all_models_from_local(
            data_dir=os.path.join(project_root, "data"),
            output_dir=output_dir,
        )

    output_log = training_log.getvalue().strip()

    return {
        "message": "Models trained successfully and forecast outputs were refreshed.",
        "output": output_log,
        "summary": training_summary,
    }
