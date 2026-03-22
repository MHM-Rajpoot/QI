import time
from pathlib import Path

from services.admin_jobs import BackgroundJobManager, train_models_job


def test_background_job_manager_runs_jobs_inside_app_context(app):
    manager = BackgroundJobManager(max_workers=1)

    try:
        job = manager.submit(
            "Context check",
            lambda: {"app_name": app.name, "root_path": __import__("flask").current_app.root_path},
            app=app,
        )

        deadline = time.time() + 3
        latest = job
        while time.time() < deadline:
            latest = manager.get_job(job["id"]) or latest
            if latest["status"] in {"completed", "failed"}:
                break
            time.sleep(0.05)

        assert latest["status"] == "completed"
        assert latest["result"]["app_name"] == app.name
        assert latest["result"]["root_path"] == app.root_path
    finally:
        manager._executor.shutdown(wait=True)


def test_train_models_job_uses_supported_training_entry_point(tmp_path, monkeypatch):
    project_root = Path(tmp_path)
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "enrolment_total.csv").write_text("ACADEMIC_YEAR,LEARNER_COUNT\n24/25,10\n", encoding="utf-8")

    captured = {}

    def fake_train_all_models_from_local(data_dir, output_dir, forecast_periods=3):
        captured["data_dir"] = data_dir
        captured["output_dir"] = output_dir
        captured["forecast_periods"] = forecast_periods
        print("training ran")
        return {"trained_models": ["ARIMA", "SARIMA", "LSTM"]}

    monkeypatch.setattr("scripts.train_local.train_all_models_from_local", fake_train_all_models_from_local)

    result = train_models_job(project_root=str(project_root))

    assert captured["data_dir"] == str(project_root / "data")
    assert captured["output_dir"] == str(project_root / "saved_models")
    assert captured["forecast_periods"] == 3
    assert "training ran" in result["output"]
    assert result["summary"]["trained_models"] == ["ARIMA", "SARIMA", "LSTM"]
