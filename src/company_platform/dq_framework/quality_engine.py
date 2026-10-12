from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DQ_VERSION = "23.0.0"
VALID_SEVERITIES = {"INFO", "WARNING", "FATAL"}

PROVENANCE_COLUMNS = [
    "source_event_id",
    "source_run_id",
    "source_document_id",
    "source_document_sha256",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and not value.strip()


def _missing_mask(series: pd.Series) -> pd.Series:
    mask = series.isna()
    string_values = series.astype("string")
    return mask | string_values.str.strip().eq("").fillna(False)


@dataclass
class DQCheck:
    name: str
    passed: bool
    severity: str
    message: str
    artifact: str | None = None
    company_id: str | None = None

    def __post_init__(self) -> None:
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(
                f"Invalid DQ severity '{self.severity}'. "
                f"Expected one of {sorted(VALID_SEVERITIES)}."
            )

    def __bool__(self) -> bool:
        return self.passed


@dataclass
class DQReport:
    dq_version: str = DQ_VERSION
    checks: list[DQCheck] = field(default_factory=list)

    def add(self, check: DQCheck) -> DQCheck:
        self.checks.append(check)
        return check

    def add_check(self, check: DQCheck) -> DQCheck:
        return self.add(check)

    @property
    def fatal_failures(self) -> list[DQCheck]:
        return [
            check
            for check in self.checks
            if not check.passed and check.severity == "FATAL"
        ]

    @property
    def warnings(self) -> list[DQCheck]:
        return [
            check
            for check in self.checks
            if check.severity == "WARNING"
        ]

    @property
    def promotion_decision(self) -> str:
        return (
            "BLOCKED"
            if self.fatal_failures
            else "PROMOTE"
        )

    def summary(self) -> dict[str, Any]:
        return {
            "dq_version": self.dq_version,
            "validation_checks": len(self.checks),
            "passed_checks": sum(check.passed for check in self.checks),
            "failed_checks": sum(not check.passed for check in self.checks),
            "fatal_failures": len(self.fatal_failures),
            "controlled_warnings": sum(
                check.severity == "WARNING" for check in self.checks
            ),
            "promotion_decision": self.promotion_decision,
        }

    def validation_dataframe(self) -> pd.DataFrame:
        generated_at = utc_now_iso()
        rows = []

        for check in self.checks:
            rows.append(
                {
                    "check_name": check.name,
                    "passed": bool(check.passed),
                    "severity": check.severity,
                    "message": check.message,
                    "artifact": check.artifact,
                    "company_id": check.company_id,
                    "dq_version": self.dq_version,
                    "generated_at_utc": generated_at,
                }
            )

        return pd.DataFrame(
            rows,
            columns=[
                "check_name",
                "passed",
                "severity",
                "message",
                "artifact",
                "company_id",
                "dq_version",
                "generated_at_utc",
            ],
        )


def check_required_columns(
    report: DQReport,
    df: pd.DataFrame,
    required_columns: Iterable[str],
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    required = list(required_columns)
    missing = [column for column in required if column not in df.columns]

    check = DQCheck(
        name="REQUIRED_COLUMNS",
        passed=not missing,
        severity="FATAL" if missing else "INFO",
        message=(
            f"Missing required columns: {missing}"
            if missing
            else f"All {len(required)} required columns are present."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_non_empty(
    report: DQReport,
    df: pd.DataFrame,
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    passed = not df.empty

    check = DQCheck(
        name="NON_EMPTY",
        passed=passed,
        severity="FATAL" if not passed else "INFO",
        message=(
            "Artifact is empty."
            if not passed
            else f"Artifact contains {len(df)} row(s)."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_unique_grain(
    report: DQReport,
    df: pd.DataFrame,
    keys: list[str],
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    missing_keys = [key for key in keys if key not in df.columns]

    if missing_keys:
        check = DQCheck(
            name="UNIQUE_GRAIN",
            passed=False,
            severity="FATAL",
            message=f"Grain columns missing: {missing_keys}",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    duplicate_mask = df.duplicated(subset=keys, keep=False)
    duplicate_rows = int(duplicate_mask.sum())

    check = DQCheck(
        name="UNIQUE_GRAIN",
        passed=duplicate_rows == 0,
        severity="FATAL" if duplicate_rows else "INFO",
        message=(
            f"Found {duplicate_rows} duplicate grain row(s) for keys {keys}."
            if duplicate_rows
            else f"Unique grain enforced for keys {keys}."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_no_duplicate_values(
    report: DQReport,
    df: pd.DataFrame,
    column: str,
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    if column not in df.columns:
        check = DQCheck(
            name="NO_DUPLICATE_VALUES",
            passed=False,
            severity="FATAL",
            message=f"Column '{column}' is missing.",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    duplicate_count = int(df[column].duplicated(keep=False).sum())

    check = DQCheck(
        name="NO_DUPLICATE_VALUES",
        passed=duplicate_count == 0,
        severity="FATAL" if duplicate_count else "INFO",
        message=(
            f"Column '{column}' contains {duplicate_count} duplicate row(s)."
            if duplicate_count
            else f"Column '{column}' contains no duplicate values."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_company_identity(
    report: DQReport,
    df: pd.DataFrame,
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    required = {"cik", "company_id"}
    missing = sorted(required - set(df.columns))

    if missing:
        check = DQCheck(
            name="COMPANY_IDENTITY",
            passed=False,
            severity="FATAL",
            message=f"Identity columns missing: {missing}",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    violations: list[str] = []

    for _, row in df.iterrows():
        cik = str(row["cik"]).strip().zfill(10)
        expected = f"SEC_{cik}"
        actual = str(row["company_id"]).strip()

        if actual != expected:
            violations.append(
                f"cik={cik}: expected={expected}, actual={actual}"
            )

    check = DQCheck(
        name="COMPANY_IDENTITY",
        passed=not violations,
        severity="FATAL" if violations else "INFO",
        message=(
            f"Company identity violations: {violations[:20]}"
            if violations
            else "CIK and company_id identity is consistent."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_value_set(
    report: DQReport,
    df: pd.DataFrame,
    column: str,
    allowed_values: set[str],
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    if column not in df.columns:
        check = DQCheck(
            name="ALLOWED_VALUES",
            passed=False,
            severity="FATAL",
            message=f"Column '{column}' is missing.",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    actual_values = {
        str(value).strip()
        for value in df[column].dropna().tolist()
    }
    invalid = sorted(actual_values - set(allowed_values))

    check = DQCheck(
        name="ALLOWED_VALUES",
        passed=not invalid,
        severity="FATAL" if invalid else "INFO",
        message=(
            f"Invalid values in '{column}': {invalid}"
            if invalid
            else f"Column '{column}' conforms to the allowed value set."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_reconciliation(
    report: DQReport,
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    key: str,
    columns: list[str],
    artifact: str,
    right_artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    required_left = {key, *columns}
    required_right = {key, *columns}

    missing_left = sorted(required_left - set(left.columns))
    missing_right = sorted(required_right - set(right.columns))

    if missing_left or missing_right:
        check = DQCheck(
            name="RECONCILIATION",
            passed=False,
            severity="FATAL",
            message=(
                f"Missing reconciliation columns: "
                f"left={missing_left}, right={missing_right}"
            ),
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    left_index = left.set_index(key)
    right_index = right.set_index(key)

    left_keys = set(left_index.index)
    right_keys = set(right_index.index)

    missing_right_keys = sorted(left_keys - right_keys)
    missing_left_keys = sorted(right_keys - left_keys)

    mismatches: list[str] = []

    for row_key in sorted(left_keys & right_keys, key=str):
        for column in columns:
            left_value = left_index.loc[row_key, column]
            right_value = right_index.loc[row_key, column]

            if isinstance(left_value, pd.Series):
                left_value = left_value.iloc[0]
            if isinstance(right_value, pd.Series):
                right_value = right_value.iloc[0]

            if _is_missing(left_value) and _is_missing(right_value):
                continue

            if str(left_value) != str(right_value):
                mismatches.append(
                    f"{row_key}:{column}:"
                    f"left={left_value!r},right={right_value!r}"
                )

    failures = (
        len(missing_right_keys)
        + len(missing_left_keys)
        + len(mismatches)
    )

    check = DQCheck(
        name="RECONCILIATION",
        passed=failures == 0,
        severity="FATAL" if failures else "INFO",
        message=(
            "Cross-layer reconciliation failed: "
            f"missing_right={missing_right_keys[:20]}, "
            f"missing_left={missing_left_keys[:20]}, "
            f"mismatches={mismatches[:20]}"
            if failures
            else (
                f"Reconciliation passed between '{artifact}' and "
                f"'{right_artifact}' on key '{key}'."
            )
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_provenance_completeness(
    report: DQReport,
    df: pd.DataFrame,
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    """
    Validate provenance using two supported lineage modes.

    EVENT_DRIVEN:
        all four provenance values are populated.

    BATCH/LEGACY:
        all four provenance values are intentionally absent.

    PARTIAL:
        only some provenance values exist. This is invalid and remains FATAL.
    """
    missing_columns = [
        column
        for column in PROVENANCE_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=False,
            severity="FATAL",
            message=f"Missing provenance columns: {missing_columns}",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    if df.empty:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=False,
            severity="FATAL",
            message="Cannot evaluate provenance on an empty artifact.",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    masks = {
        column: _missing_mask(df[column])
        for column in PROVENANCE_COLUMNS
    }

    complete_mask = pd.Series(True, index=df.index)
    empty_mask = pd.Series(True, index=df.index)

    for mask in masks.values():
        complete_mask &= ~mask
        empty_mask &= mask

    partial_mask = ~(complete_mask | empty_mask)

    complete_count = int(complete_mask.sum())
    empty_count = int(empty_mask.sum())
    partial_count = int(partial_mask.sum())
    total_rows = len(df)

    if partial_count:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=False,
            severity="FATAL",
            message=(
                f"Partial provenance detected in {partial_count} row(s). "
                "Each row must contain either all four provenance fields "
                "(EVENT_DRIVEN) or none of them (BATCH/LEGACY)."
            ),
            artifact=artifact,
            company_id=company_id,
        )
    elif empty_count == total_rows:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=True,
            severity="WARNING",
            message=(
                f"Artifact contains {total_rows} BATCH/LEGACY row(s). "
                "Event/document provenance is not available for these rows."
            ),
            artifact=artifact,
            company_id=company_id,
        )
    elif complete_count == total_rows:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=True,
            severity="INFO",
            message=(
                f"Complete EVENT_DRIVEN provenance present for "
                f"{complete_count} row(s)."
            ),
            artifact=artifact,
            company_id=company_id,
        )
    else:
        check = DQCheck(
            name="PROVENANCE_COMPLETENESS",
            passed=True,
            severity="WARNING",
            message=(
                f"Mixed lineage: {complete_count} EVENT_DRIVEN row(s) and "
                f"{empty_count} BATCH/LEGACY row(s)."
            ),
            artifact=artifact,
            company_id=company_id,
        )

    report.add(check)
    return check


def check_provenance_consistency(
    report: DQReport,
    df: pd.DataFrame,
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    """
    Enforce row-level provenance consistency.

    Valid:
      - all four populated
      - all four missing

    Invalid:
      - partial provenance
    """
    missing_columns = [
        column
        for column in PROVENANCE_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        check = DQCheck(
            name="PROVENANCE_CONSISTENCY",
            passed=False,
            severity="FATAL",
            message=f"Missing provenance columns: {missing_columns}",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    populated_count = pd.Series(0, index=df.index, dtype="int64")

    for column in PROVENANCE_COLUMNS:
        populated_count += (~_missing_mask(df[column])).astype("int64")

    partial_mask = (
        (populated_count > 0)
        & (populated_count < len(PROVENANCE_COLUMNS))
    )

    partial_count = int(partial_mask.sum())
    event_driven_count = int(
        (populated_count == len(PROVENANCE_COLUMNS)).sum()
    )
    legacy_count = int((populated_count == 0).sum())

    check = DQCheck(
        name="PROVENANCE_CONSISTENCY",
        passed=partial_count == 0,
        severity="FATAL" if partial_count else "INFO",
        message=(
            f"{partial_count} row(s) contain partial provenance."
            if partial_count
            else (
                f"Provenance consistency passed: "
                f"{event_driven_count} EVENT_DRIVEN row(s), "
                f"{legacy_count} BATCH/LEGACY row(s)."
            )
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_freshness(
    report: DQReport,
    df: pd.DataFrame,
    column: str,
    *,
    max_age_hours: float,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    if column not in df.columns:
        check = DQCheck(
            name="FRESHNESS",
            passed=False,
            severity="FATAL",
            message=f"Freshness column '{column}' is missing.",
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    timestamps = pd.to_datetime(
        df[column],
        errors="coerce",
        utc=True,
    )

    invalid_count = int(timestamps.isna().sum())

    if invalid_count:
        check = DQCheck(
            name="FRESHNESS",
            passed=False,
            severity="FATAL",
            message=(
                f"{invalid_count} row(s) contain invalid/missing "
                f"timestamps in '{column}'."
            ),
            artifact=artifact,
            company_id=company_id,
        )
        report.add(check)
        return check

    now = pd.Timestamp.now(tz="UTC")
    age_hours = (now - timestamps).dt.total_seconds() / 3600.0
    stale_count = int((age_hours > max_age_hours).sum())

    check = DQCheck(
        name="FRESHNESS",
        passed=stale_count == 0,
        severity="FATAL" if stale_count else "INFO",
        message=(
            f"{stale_count} row(s) exceed the {max_age_hours}-hour freshness limit."
            if stale_count
            else f"All rows are within the {max_age_hours}-hour freshness limit."
        ),
        artifact=artifact,
        company_id=company_id,
    )
    report.add(check)
    return check


def check_provenance_columns(
    report: DQReport,
    df: pd.DataFrame,
    provenance_columns: list[str],
    *,
    artifact: str,
    company_id: str | None = None,
) -> DQCheck:
    return check_provenance_completeness(
        report,
        df,
        artifact=artifact,
        company_id=company_id,
    )


def audit_parquet_artifact(
    report: DQReport,
    path: Path,
    *,
    required_columns: list[str],
    grain_keys: list[str] | None = None,
    identity_check: bool = False,
    provenance_columns: list[str] | None = None,
) -> pd.DataFrame | None:
    artifact = str(path)

    if not path.exists():
        report.add(
            DQCheck(
                name="ARTIFACT_EXISTS",
                passed=False,
                severity="FATAL",
                message=f"Artifact does not exist: {path}",
                artifact=artifact,
            )
        )
        return None

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        report.add(
            DQCheck(
                name="ARTIFACT_READABLE",
                passed=False,
                severity="FATAL",
                message=f"Unable to read Parquet artifact: {exc}",
                artifact=artifact,
            )
        )
        return None

    report.add(
        DQCheck(
            name="ARTIFACT_EXISTS",
            passed=True,
            severity="INFO",
            message=f"Artifact exists: {path}",
            artifact=artifact,
        )
    )

    report.add(
        DQCheck(
            name="ARTIFACT_READABLE",
            passed=True,
            severity="INFO",
            message=f"Artifact loaded successfully with {len(df)} row(s).",
            artifact=artifact,
        )
    )

    check_non_empty(
        report,
        df,
        artifact=artifact,
    )

    check_required_columns(
        report,
        df,
        required_columns,
        artifact=artifact,
    )

    if grain_keys:
        check_unique_grain(
            report,
            df,
            grain_keys,
            artifact=artifact,
        )

    if identity_check:
        check_company_identity(
            report,
            df,
            artifact=artifact,
        )

    if provenance_columns:
        # Keep this check mode-aware. The supplied list is retained for
        # backward compatibility with callers, but the canonical four-field
        # contract is enforced by check_provenance_completeness().
        check_provenance_completeness(
            report,
            df,
            artifact=artifact,
        )

    return df


def write_report(
    report: DQReport,
    output: Path,
) -> dict[str, Path]:
    output.mkdir(parents=True, exist_ok=True)

    validation_path = output / "dq_validation_checks.parquet"
    summary_path = output / "dq_summary.parquet"
    exceptions_path = output / "dq_exceptions.parquet"
    promotion_path = output / "dq_promotion.parquet"

    validation_df = report.validation_dataframe()
    validation_df.to_parquet(validation_path, index=False)

    summary = report.summary()
    summary_df = pd.DataFrame([{
        **summary,
        "generated_at_utc": utc_now_iso(),
    }])
    summary_df.to_parquet(summary_path, index=False)

    exceptions_df = validation_df[
        (~validation_df["passed"])
        | (validation_df["severity"] == "WARNING")
    ].copy()
    exceptions_df.to_parquet(exceptions_path, index=False)

    promotion_df = pd.DataFrame([{
        "dq_version": report.dq_version,
        "promotion_decision": summary["promotion_decision"],
        "fatal_failure_count": summary["fatal_failures"],
        "controlled_warning_count": summary["controlled_warnings"],
        "generated_at_utc": utc_now_iso(),
    }])
    promotion_df.to_parquet(promotion_path, index=False)

    return {
        "validation_checks": validation_path,
        "summary": summary_path,
        "exceptions": exceptions_path,
        "promotion": promotion_path,
    }
