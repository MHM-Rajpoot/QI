def test_refresh_data_job_callback_can_run_outside_app_context(client, app, monkeypatch):
    queued = {}

    class JobManagerStub:
        def submit(self, name, func, app=None):
            queued["name"] = name
            queued["func"] = func
            queued["app"] = app
            return {"id": "job-123", "status": "queued", "message": f"{name} queued"}

    def fake_refresh_all_data_job(**kwargs):
        return kwargs

    monkeypatch.setitem(app.extensions, "job_manager", JobManagerStub())
    monkeypatch.setattr("routes.api.refresh_all_data_job", fake_refresh_all_data_job)

    response = client.post("/api/data/refresh")
    payload = response.get_json()

    assert response.status_code == 202
    assert payload["status"] == "success"
    assert queued["name"] == "Data refresh"
    assert queued["app"] is app

    queued_result = queued["func"]()
    assert queued_result["root_path"] == app.root_path
    assert queued_result["data_dir"].endswith("data")


def test_health_endpoint_includes_version_headers(client):
    response = client.get("/api/health")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["status"] == "success"
    assert payload["version"]["api"] == "2026.03.0"
    assert response.headers["X-Contract-Id"] == "health.status"


def test_contract_registry_endpoint_is_discoverable(client):
    response = client.get("/api/contracts")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["status"] == "success"
    assert payload["data"]["contracts"]
    assert any(item["id"] == "contracts.registry" for item in payload["data"]["contracts"])


def test_credentials_view_returns_editable_payload(client):
    response = client.get("/api/credentials/view")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["status"] == "success"
    assert payload["data"]["editable"] is True


def test_credentials_save_persists_connection_settings(client, app):
    response = client.post(
        "/api/credentials/save",
        json={
            "account": "demo-account",
            "user": "demo-user",
            "password": "demo-password",
            "role": "demo-role",
            "database": "demo-database",
            "warehouse": "demo-warehouse",
        },
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["status"] == "success"
    assert payload["data"]["account"] == "demo-account"
    assert payload["data"]["password"] == "demo-password"

    with open(app.config["SNOWFLAKE_CONFIG_FILE"], "r", encoding="utf-8") as file_obj:
        saved_contents = file_obj.read()

    assert "[connections.my_example_connection]" in saved_contents
    assert "account = demo-account" in saved_contents
    assert "password = demo-password" in saved_contents


def test_programme_plans_data_validates_page_size(client):
    response = client.post(
        "/api/programme-plans/data",
        json={"page": 1, "page_size": 999, "search": "", "filters": {}},
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["status"] == "error"
    assert payload["error"]["field"] == "page_size"


def test_programme_plans_data_returns_versioned_paginated_payload(client):
    response = client.post(
        "/api/programme-plans/data",
        json={"page": 1, "page_size": 2, "search": "care", "filters": {"Approval Status": "Completed"}},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["status"] == "success"
    assert payload["version"]["contract"]["id"] == "programme_plans.page"
    assert payload["version"]["contract"]["version"] == "v2"
    assert payload["meta"]["pagination"]["page_size"] == 2
    assert payload["meta"]["pagination"]["total_rows"] == 2
