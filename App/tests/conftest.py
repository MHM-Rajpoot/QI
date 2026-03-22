import os
from pathlib import Path
import sys

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("SECRET_KEY", "test-import-secret")

from app import create_app
from scripts.config import Config


@pytest.fixture()
def sample_programme_plans_csv(tmp_path: Path) -> Path:
    df = pd.DataFrame([
        {
            "CA Name": "Health, Public Services and Care",
            "Prog Name": "Adult Care Diploma",
            "Approval Status": "Completed",
            "Infill": "N",
            "Site": "Main Campus",
            "Level": "Level 3",
            "Parent": "College A",
            "Course ID": "AC-001",
        },
        {
            "CA Name": "Engineering and Manufacturing Technologies",
            "Prog Name": "Machining Basics",
            "Approval Status": "Continuing",
            "Infill": "N",
            "Site": "Skills Centre",
            "Level": "Level 2",
            "Parent": "College B",
            "Course ID": "EN-002",
        },
        {
            "CA Name": "Health, Public Services and Care",
            "Prog Name": "Nursing Pathway",
            "Approval Status": "Completed",
            "Infill": "N",
            "Site": "Main Campus",
            "Level": "Level 3",
            "Parent": "College A",
            "Course ID": "NU-003",
        },
    ])
    path = tmp_path / "programme_plans.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture()
def app(sample_programme_plans_csv: Path):
    class TestConfig(Config):
        TESTING = True
        SECRET_KEY = "test-secret"
        PROGRAMME_PLANS_CSV_FILE = str(sample_programme_plans_csv)
        SNOWFLAKE_CONFIG_FILE = str(sample_programme_plans_csv.parent / "snowflake.txt")

    app = create_app(TestConfig)
    app.config.update(TESTING=True)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()
