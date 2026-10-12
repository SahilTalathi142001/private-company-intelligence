from __future__ import annotations

import shutil
from pathlib import Path
from textwrap import dedent


ROOT = Path(__file__).resolve().parents[1]

COMPONENTS = ROOT / "src" / "company_platform" / "integration_v2" / "components.py"
CAPABILITIES = ROOT / "src" / "company_platform" / "orchestrator" / "capabilities.py"


def backup(path: Path) -> Path:
    backup_path = path.with_suffix(path.suffix + ".bak_unified_company_view")
    shutil.copy2(path, backup_path)
    print(f"[BACKUP] {path}")
    print(f"         -> {backup_path}")
    return backup_path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)

    if count != 1:
        raise RuntimeError(
            f"{label}: expected exactly 1 occurrence, found {count}"
        )

    return text.replace(old, new, 1)


def patch_capabilities() -> None:
    if not CAPABILITIES.exists():
        raise FileNotFoundError(f"Capabilities file not found: {CAPABILITIES}")

    text = CAPABILITIES.read_text(encoding="utf-8")

    marker = "'company_discovery':"

    if "'unified_company_view':" in text:
        print("[CAPABILITIES] unified_company_view already registered")
        return

    idx = text.find(marker)

    if idx < 0:
        raise RuntimeError(
            "Could not locate company_discovery capability in capabilities.py"
        )

    line_end = text.find("\n", idx)

    if line_end < 0:
        raise RuntimeError(
            "Could not determine end of company_discovery capability line"
        )

    insertion = (
        "\n"
        "    'unified_company_view': {\n"
        "        'name': 'unified_company_view',\n"
        "        'description': 'Build and refresh the unified company intelligence view.',\n"
        "        'version': '1.0.0',\n"
        "    },"
    )

    text = text[:line_end] + insertion + text[line_end:]

    CAPABILITIES.write_text(text, encoding="utf-8")

    print("[CAPABILITIES] Registered unified_company_view")


def patch_components() -> None:
    if not COMPONENTS.exists():
        raise FileNotFoundError(f"Components file not found: {COMPONENTS}")

    text = COMPONENTS.read_text(encoding="utf-8")

    # ------------------------------------------------------------------
    # 1. Add handler function
    # ------------------------------------------------------------------

    if "def _unified_company_view(" not in text:

        marker = "def _company_state_projection("

        if marker not in text:
            raise RuntimeError(
                "Could not locate _company_state_projection in components.py"
            )

        handler = dedent(
            r'''
            def _unified_company_view(context: ExecutionContext) -> dict:
                """
                Build the first-class Unified Company View.

                Design:
                - Step 20 investment_profile is the authoritative base.
                - Existing UCV enrichment is preserved where available.
                - Company state/event fields are added when available.
                - Missing event/state information remains explicit NO_DATA /
                  INSUFFICIENT_DATA rather than being fabricated.
                - The output universe is exactly the Step 20 universe.
                - All Step 20 screening columns are preserved.
                """

                import os
                import tempfile

                import pandas as pd

                data_root = Path(
                    os.getenv(
                        "PRIVATE_COMPANY_INTELLIGENCE_DATA",
                        str(DATA),
                    )
                )

                investment_profile_path = (
                    data_root
                    / "gold"
                    / "unified_investment_intelligence"
                    / "investment_profile"
                    / "investment_profile.parquet"
                )

                investment_signals_path = (
                    data_root
                    / "gold"
                    / "unified_investment_intelligence"
                    / "investment_signals"
                    / "investment_signals.parquet"
                )

                ucv_path = (
                    data_root
                    / "gold"
                    / "unified_company_intelligence"
                    / "unified_company_view"
                    / "unified_company_view.parquet"
                )

                if not investment_profile_path.exists():
                    raise FileNotFoundError(
                        f"Step 20 investment profile not found: "
                        f"{investment_profile_path}"
                    )

                profile = pd.read_parquet(investment_profile_path)

                if profile.empty:
                    raise RuntimeError(
                        "Step 20 investment profile is empty; "
                        "cannot build unified_company_view"
                    )

                if "cik" not in profile.columns:
                    raise RuntimeError(
                        "Step 20 investment profile does not contain 'cik'"
                    )

                profile = profile.copy()

                profile["cik"] = (
                    profile["cik"]
                    .astype(str)
                    .str.replace(r"\\.0$", "", regex=True)
                    .str.strip()
                    .str.zfill(10)
                )

                # ------------------------------------------------------
                # Validate Step 20 universe
                # ------------------------------------------------------

                profile = profile.drop_duplicates(
                    subset=["cik"],
                    keep="last",
                )

                step20_ciks = set(profile["cik"].tolist())

                if not step20_ciks:
                    raise RuntimeError(
                        "Step 20 investment profile contains no valid CIKs"
                    )

                # ------------------------------------------------------
                # Investment signals
                # ------------------------------------------------------

                signals_by_cik = {}

                if investment_signals_path.exists():
                    signals = pd.read_parquet(investment_signals_path)

                    if not signals.empty and "cik" in signals.columns:
                        signals = signals.copy()

                        signals["cik"] = (
                            signals["cik"]
                            .astype(str)
                            .str.replace(r"\\.0$", "", regex=True)
                            .str.strip()
                            .str.zfill(10)
                        )

                        for cik, group in signals.groupby("cik"):
                            signal_types = []

                            if "signal_type" in group.columns:
                                signal_types = sorted(
                                    {
                                        str(value)
                                        for value in group["signal_type"].dropna()
                                        if str(value).strip()
                                    }
                                )

                            signals_by_cik[cik] = {
                                "investment_signal_count": int(len(group)),
                                "investment_signal_types": (
                                    "|".join(signal_types)
                                    if signal_types
                                    else None
                                ),
                            }

                # ------------------------------------------------------
                # Existing UCV
                # ------------------------------------------------------

                existing_ucv = None

                if ucv_path.exists():
                    try:
                        existing_ucv = pd.read_parquet(ucv_path)

                        if not existing_ucv.empty and "cik" in existing_ucv.columns:
                            existing_ucv = existing_ucv.copy()

                            existing_ucv["cik"] = (
                                existing_ucv["cik"]
                                .astype(str)
                                .str.replace(r"\\.0$", "", regex=True)
                                .str.strip()
                                .str.zfill(10)
                            )

                            existing_ucv = existing_ucv.drop_duplicates(
                                subset=["cik"],
                                keep="last",
                            )

                    except Exception as exc:
                        # Existing UCV is enrichment only. A corrupt/stale
                        # UCV must not prevent regeneration from Step 20.
                        print(
                            "[UCV] Existing UCV could not be read; "
                            f"rebuilding from Step 20: {exc}"
                        )
                        existing_ucv = None

                # ------------------------------------------------------
                # Preserve all Step 20 fields
                # ------------------------------------------------------

                result = profile.copy()

                # Existing UCV enrichment is merged only where it does not
                # overwrite authoritative Step 20 intelligence fields.
                if existing_ucv is not None:
                    enrichment_columns = [
                        column
                        for column in existing_ucv.columns
                        if column != "cik"
                        and column not in result.columns
                    ]

                    if enrichment_columns:
                        enrichment = existing_ucv[
                            ["cik"] + enrichment_columns
                        ].copy()

                        result = result.merge(
                            enrichment,
                            on="cik",
                            how="left",
                            validate="one_to_one",
                        )

                # ------------------------------------------------------
                # Add/refresh investment signal information
                # ------------------------------------------------------

                if signals_by_cik:
                    result["investment_signal_count"] = result["cik"].map(
                        lambda cik: signals_by_cik.get(cik, {}).get(
                            "investment_signal_count"
                        )
                    )

                    result["investment_signal_types"] = result["cik"].map(
                        lambda cik: signals_by_cik.get(cik, {}).get(
                            "investment_signal_types"
                        )
                    )

                # ------------------------------------------------------
                # Company identity
                # ------------------------------------------------------

                if "company_id" not in result.columns:
                    result["company_id"] = (
                        "SEC_" + result["cik"].astype(str)
                    )

                else:
                    result["company_id"] = result.apply(
                        lambda row: (
                            row["company_id"]
                            if pd.notna(row["company_id"])
                            and str(row["company_id"]).strip()
                            else f"SEC_{row['cik']}"
                        ),
                        axis=1,
                    )

                # ------------------------------------------------------
                # Company state / event enrichment
                # ------------------------------------------------------

                defaults = {
                    "company_state_status": "NO_DATA",
                    "company_state_version": "0.0.0",
                    "capital_activity_detected": None,
                    "recurring_capital_activity": None,
                    "capital_raise_execution": None,
                    "event_decision_posture": "INSUFFICIENT_DATA",
                    "event_decision_confidence": None,
                    "event_decision_id": None,
                    "source_event_id": None,
                    "source_run_id": None,
                    "source_document_id": None,
                    "source_document_sha256": None,
                    "latest_funding_date": None,
                    "latest_total_offering_amount_usd": None,
                    "latest_amount_sold_usd": None,
                    "cumulative_offering_amount_usd": None,
                    "cumulative_amount_sold_usd": None,
                    "observed_funding_event_count": None,
                    "unified_company_view_version": "1.0.0",
                }

                for column, default_value in defaults.items():
                    if column not in result.columns:
                        result[column] = default_value

                # ------------------------------------------------------
                # Do not fabricate event values.
                #
                # If an existing UCV row contains actual values, preserve
                # them. If not, leave them as explicit unknowns.
                # ------------------------------------------------------

                if existing_ucv is not None and not existing_ucv.empty:
                    event_columns = [
                        column
                        for column in defaults
                        if column in existing_ucv.columns
                    ]

                    if event_columns:
                        existing_event = existing_ucv[
                            ["cik"] + event_columns
                        ].copy()

                        existing_event = existing_event.drop_duplicates(
                            subset=["cik"],
                            keep="last",
                        )

                        existing_event = existing_event.set_index("cik")

                        for column in event_columns:
                            if column not in result.columns:
                                result[column] = None

                            values = result["cik"].map(
                                existing_event[column]
                            )

                            # Existing actual values win over defaults.
                            result[column] = values.combine_first(
                                result[column]
                            )

                # ------------------------------------------------------
                # Correct event state defaults after merge
                # ------------------------------------------------------

                result["company_state_status"] = result[
                    "company_state_status"
                ].fillna("NO_DATA")

                result["company_state_version"] = result[
                    "company_state_version"
                ].fillna("0.0.0")

                result["event_decision_posture"] = result[
                    "event_decision_posture"
                ].fillna("INSUFFICIENT_DATA")

                result["unified_company_view_version"] = result[
                    "unified_company_view_version"
                ].fillna("1.0.0")

                # ------------------------------------------------------
                # Funding fields
                #
                # These remain distinct from valuation.
                # SEC/Form D capital must never be automatically treated
                # as company valuation.
                # ------------------------------------------------------

                if "sec_capital_treated_as_valuation" not in result.columns:
                    result["sec_capital_treated_as_valuation"] = False

                if "valuation_fabrication_prevented" not in result.columns:
                    result["valuation_fabrication_prevented"] = True

                # ------------------------------------------------------
                # Ensure deterministic ordering
                # ------------------------------------------------------

                result = result.sort_values(
                    by=["cik"],
                    kind="stable",
                ).reset_index(drop=True)

                # ------------------------------------------------------
                # Hard invariants
                # ------------------------------------------------------

                output_ciks = set(result["cik"].tolist())

                if output_ciks != step20_ciks:
                    missing = sorted(step20_ciks - output_ciks)
                    extra = sorted(output_ciks - step20_ciks)

                    raise RuntimeError(
                        "Unified company view universe mismatch. "
                        f"Missing={missing[:20]} "
                        f"Extra={extra[:20]}"
                    )

                if result["cik"].duplicated().any():
                    duplicates = (
                        result.loc[
                            result["cik"].duplicated(keep=False),
                            "cik",
                        ]
                        .astype(str)
                        .unique()
                        .tolist()
                    )

                    raise RuntimeError(
                        "Unified company view contains duplicate CIK rows: "
                        f"{duplicates[:20]}"
                    )

                # ------------------------------------------------------
                # Atomic write
                # ------------------------------------------------------

                ucv_path.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )

                fd, temp_name = tempfile.mkstemp(
                    prefix="unified_company_view_",
                    suffix=".parquet",
                    dir=str(ucv_path.parent),
                )

                os.close(fd)

                temp_path = Path(temp_name)

                try:
                    result.to_parquet(
                        temp_path,
                        index=False,
                    )

                    temp_path.replace(ucv_path)

                finally:
                    if temp_path.exists():
                        temp_path.unlink()

                print(
                    "[UCV] Built unified_company_view: "
                    f"rows={len(result)}, "
                    f"columns={len(result.columns)}, "
                    f"ciks={len(output_ciks)}"
                )

                return {
                    "status": "SUCCESS",
                    "component": "unified_company_view",
                    "rows": int(len(result)),
                    "columns": int(len(result.columns)),
                    "companies": int(len(output_ciks)),
                    "output_path": str(ucv_path),
                    "step20_universe_match": True,
                    "valuation_fabrication_prevented": True,
                }

            '''
        )

        text = text.replace(
            marker,
            handler + "\n\n" + marker,
            1,
        )

        print("[COMPONENTS] Added _unified_company_view handler")

    else:
        print("[COMPONENTS] _unified_company_view already exists")

    # ------------------------------------------------------------------
    # 2. Register HANDLERS entry
    # ------------------------------------------------------------------

    if "'unified_company_view':" not in text:

        marker = "'company_discovery': _a8_adapter(_company_discovery),"

        if marker not in text:
            raise RuntimeError(
                "Could not locate company_discovery HANDLERS entry"
            )

        replacement = (
            marker
            + "\n"
            + "    'unified_company_view': _a8_adapter(_unified_company_view),"
        )

        text = text.replace(
            marker,
            replacement,
            1,
        )

        print("[COMPONENTS] Registered unified_company_view handler")

    else:
        print("[COMPONENTS] unified_company_view handler already registered")

    COMPONENTS.write_text(text, encoding="utf-8")

    print("[COMPONENTS] Updated components.py")


def main() -> None:
    print("=" * 80)
    print("UNIFIED COMPANY VIEW FIX")
    print("=" * 80)

    if not COMPONENTS.exists():
        raise FileNotFoundError(COMPONENTS)

    if not CAPABILITIES.exists():
        raise FileNotFoundError(CAPABILITIES)

    print()
    print("[1/4] Creating backups...")
    backup(COMPONENTS)
    backup(CAPABILITIES)

    print()
    print("[2/4] Registering capability...")
    patch_capabilities()

    print()
    print("[3/4] Patching component...")
    patch_components()

    print()
    print("[4/4] Complete")
    print()
    print("Next commands:")
    print()
    print("  python -m compileall src")
    print()
    print(
        "  python -m pytest "
        "tests\\integration\\test_new_sec_filing_event.py -q"
    )
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()