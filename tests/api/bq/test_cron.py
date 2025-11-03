from datetime import datetime, timedelta, timezone

from api.bq import cron


def test_adjust_since_for_overlap_none():
    assert cron._adjust_since_for_overlap(None) is None


def test_adjust_since_for_overlap_subtracts_one_second():
    ts = datetime(2025, 11, 3, 10, 0, 0)
    adjusted = cron._adjust_since_for_overlap(ts)
    assert adjusted == ts - timedelta(seconds=1)


def test_format_sqlite_datetime_naive():
    ts = datetime(2025, 11, 3, 10, 0, 0)
    s = cron._format_sqlite_datetime(ts)
    assert s == "2025-11-03 10:00:00"


def test_format_sqlite_datetime_tzaware():
    ts = datetime(2025, 11, 3, 10, 0, 0, tzinfo=timezone.utc)
    s = cron._format_sqlite_datetime(ts)
    # Expect UTC naive representation
    assert s == "2025-11-03 10:00:00"


def test_compute_rows_max_activity_ts():
    rows = [
        {
            "id": 1,
            "created_at": "2025-11-03 10:00:00",
            "updated_at": None,
            "deleted_at": None,
        },
        {
            "id": 2,
            "created_at": "2025-11-03 10:01:00",
            "updated_at": "2025-11-03 10:02:00",
            "deleted_at": None,
        },
        {
            "id": 3,
            "created_at": "2025-11-03 10:01:30",
            "updated_at": None,
            "deleted_at": "2025-11-03 10:03:00",
        },
    ]
    max_ts = cron._compute_rows_max_activity_ts(rows)
    assert max_ts == datetime(2025, 11, 3, 10, 3, 0)


def test_infer_bq_field_type():
    assert cron._infer_bq_field_type("id", 1) == "INT64"
    assert cron._infer_bq_field_type("is_feedback_shown", True) == "BOOL"
    assert cron._infer_bq_field_type("score", 1.5) == "FLOAT64"
    assert cron._infer_bq_field_type("data", {"a": 1}) == "STRING"
    assert cron._infer_bq_field_type("created_at", None) == "TIMESTAMP"


def test_build_staging_table_id():
    tid = cron._build_staging_table_id("proj.dataset.table")
    assert tid == "proj.dataset._staging_table"
