import textwrap

from utils.credentials import load_snowflake_settings


def test_load_snowflake_settings_uses_config_file_when_env_missing(tmp_path, monkeypatch):
    for env_name in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE",
    ):
        monkeypatch.delenv(env_name, raising=False)

    config_path = tmp_path / "passcode.txt"
    config_path.write_text(
        textwrap.dedent(
            """
            [connections.my_example_connection]
            account = "demo-account"
            user = "demo-user"
            password = "demo-password"
            role = "demo-role"
            database = "demo-database"
            warehouse = "<none selected>"
            """
        ).strip(),
        encoding="utf-8",
    )

    payload = load_snowflake_settings(str(config_path))

    assert payload["source"] == "config_file"
    assert payload["settings"]["account"] == "demo-account"
    assert payload["settings"]["warehouse"] == "<none selected>"
