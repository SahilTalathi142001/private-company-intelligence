from datetime import datetime, timezone, timedelta

import pandas as pd

from company_platform.freshness.freshness import (
    build_company_freshness,
    build_summary,
    evaluate_status,
    latest_sec_coverage,
)


def test_fresh_status():
    now = datetime(
        2026,
        10,
        7,
        10,
        0,
        tzinfo=timezone.utc,
    )

    status, age = evaluate_status(
        refreshed_at=now - timedelta(
            minutes=30
        ),
        coverage_status="SUCCESS",
        now=now,
        max_age_minutes=120,
    )

    assert status == "FRESH"
    assert age == 30.0


def test_stale_status():
    now = datetime(
        2026,
        10,
        7,
        10,
        0,
        tzinfo=timezone.utc,
    )

    status, age = evaluate_status(
        refreshed_at=now - timedelta(
            minutes=180
        ),
        coverage_status="SUCCESS",
        now=now,
        max_age_minutes=120,
    )

    assert status == "STALE"
    assert age == 180.0


def test_never_refreshed_status():
    now = datetime(
        2026,
        10,
        7,
        10,
        0,
        tzinfo=timezone.utc,
    )

    status, age = evaluate_status(
        refreshed_at=None,
        coverage_status=None,
        now=now,
        max_age_minutes=120,
    )

    assert status == "NEVER_REFRESHED"
    assert age is None


def test_failed_refresh_is_error():
    now = datetime(
        2026,
        10,
        7,
        10,
        0,
        tzinfo=timezone.utc,
    )

    status, age = evaluate_status(
        refreshed_at=now,
        coverage_status="FAILED",
        now=now,
        max_age_minutes=120,
    )

    assert status == "ERROR"
    assert age is None


def test_latest_coverage_per_company():
    coverage = pd.DataFrame(
        [
            {
                "company_id": "SEC_0000000001",
                "cik": "0000000001",
                "status": "SUCCESS",
                "refreshed_at_utc":
                    "2026-10-07T08:00:00+00:00",
            },
            {
                "company_id": "SEC_0000000001",
                "cik": "0000000001",
                "status": "SUCCESS",
                "refreshed_at_utc":
                    "2026-10-07T09:00:00+00:00",
            },
        ]
    )

    result = latest_sec_coverage(
        coverage
    )

    assert len(result) == 1
    assert (
        result.iloc[0]["refreshed_at_utc"]
        == "2026-10-07T09:00:00+00:00"
    )


def test_company_freshness_covers_missing_company():
    now = datetime(
        2026,
        10,
        7,
        10,
        0,
        tzinfo=timezone.utc,
    )

    registry = pd.DataFrame(
        [
            {
                "company_id":
                    "SEC_0000000001",
                "cik":
                    "0000000001",
                "company_name":
                    "Company One",
                "ticker":
                    "ONE",
                "status":
                    "ACTIVE",
                "ingestion_enabled":
                    True,
            },
            {
                "company_id":
                    "SEC_0000000002",
                "cik":
                    "0000000002",
                "company_name":
                    "Company Two",
                "ticker":
                    "TWO",
                "status":
                    "ACTIVE",
                "ingestion_enabled":
                    True,
            },
        ]
    )

    coverage = pd.DataFrame(
        [
            {
                "company_id":
                    "SEC_0000000001",
                "cik":
                    "0000000001",
                "status":
                    "SUCCESS",
                "refreshed_at_utc":
                    "2026-10-07T09:00:00+00:00",
            }
        ]
    )

    result = build_company_freshness(
        registry,
        coverage,
        now=now,
        max_age_minutes=120,
    )

    statuses = dict(
        zip(
            result["ticker"],
            result["freshness_status"],
        )
    )

    assert statuses["ONE"] == "FRESH"
    assert (
        statuses["TWO"]
        == "NEVER_REFRESHED"
    )


def test_summary_becomes_error_if_any_company_errors():
    freshness = pd.DataFrame(
        [
            {
                "freshness_status": "FRESH"
            },
            {
                "freshness_status": "ERROR"
            },
        ]
    )

    summary = build_summary(
        freshness,
        now=datetime(
            2026,
            10,
            7,
            tzinfo=timezone.utc,
        ),
        max_age_minutes=120,
    )

    row = summary.iloc[0]

    assert (
        row["platform_freshness_status"]
        == "ERROR"
    )
    assert row["companies_evaluated"] == 2
    assert row["fresh_count"] == 1
    assert row["error_count"] == 1