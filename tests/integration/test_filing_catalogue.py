from datetime import datetime, timezone

import pandas as pd
import pytest

from company_platform.manifests.catalogue import (
    enroll_and_backfill,
    promote_local,
    publish_catalogue,
    read_incremental_since,
    run_daily,
    sync_manifests_to_s3,
)
from company_platform.manifests.lineage import retain_lineaged_rows
from sec_ingestion.canonicalize_financials import canonicalize_company
from sec_ingestion.step18_investor_ownership_intelligence import build_investor_events


def _write(path, frame):
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _registry_row(cik, name, ticker):
    return {
        "company_id": f"SEC_{cik}",
        "cik": cik,
        "company_name": name,
        "ticker": ticker,
        "source": "SEC",
        "status": "ACTIVE",
        "onboarding_status": "ACTIVE",
        "ingestion_enabled": True,
        "created_at_utc": "2026-01-01T00:00:00+00:00",
        "updated_at_utc": "2026-01-01T00:00:00+00:00",
        "last_successful_run_utc": None,
        "last_error": None,
    }


def _filing(cik, accession, filing_date, form, primary, name):
    return {
        "accession_number": accession,
        "filing_date": filing_date,
        "report_date": filing_date,
        "form": form,
        "primary_document": primary,
        "primary_doc_description": form,
        "cik": cik,
        "company_name": name,
    }


def _manifest(data, cik, accession, filing_date, form, primary, status="DOWNLOADED", error=""):
    document = data / "bronze" / "sec" / "documents" / f"cik={cik}" / accession.replace("-", "") / primary
    if status == "DOWNLOADED":
        document.parent.mkdir(parents=True, exist_ok=True)
        document.write_text("filing", encoding="utf-8")
        local_path = str(document)
    else:
        local_path = ""
    return {
        "document_id": accession,
        "company_id": f"SEC_{cik}",
        "cik": cik,
        "company_name": "Example",
        "accession_number": accession,
        "filing_date": filing_date,
        "report_date": filing_date,
        "form": form,
        "primary_document": primary,
        "document_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{primary}",
        "local_path": local_path,
        "sha256": "abc",
        "byte_count": 6 if status == "DOWNLOADED" else 0,
        "status": status,
        "discovered_at_utc": "2026-03-01T00:00:00+00:00",
        "downloaded_at_utc": "2026-03-01T00:00:00+00:00" if status == "DOWNLOADED" else "",
        "error": error,
    }


def _seed(tmp_path):
    data = tmp_path / "data"
    cik = "0000320193"
    accession = "0000320193-26-000001"
    filings = pd.DataFrame([
        _filing(cik, accession, "2026-03-01", "10-K", "aapl-2026.htm", "Apple Inc."),
        _filing(cik, "0000320193-25-000001", "2025-03-01", "10-K", "aapl-2025.htm", "Apple Inc."),
    ])
    _write(data / "silver" / "filings" / f"cik={cik}" / "filings.parquet", filings)
    manifests = pd.DataFrame([
        _manifest(data, cik, accession, "2026-03-01", "10-K", "aapl-2026.htm"),
        _manifest(data, cik, "0000320193-25-000001", "2025-03-01", "10-K", "aapl-2025.htm"),
    ])
    _write(data / "bronze" / "sec" / "documents" / f"cik={cik}" / "document_manifest.parquet", manifests)
    _write(
        data / "silver" / "platform" / "company_registry.parquet",
        pd.DataFrame([_registry_row(cik, "Apple Inc.", "AAPL")]),
    )
    _write(
        data / "silver" / "company" / "company.parquet",
        pd.DataFrame([{"cik": cik, "ticker": "AAPL", "company_name": "Apple Inc."}]),
    )
    return data


def test_catalogue_is_one_row_per_accession_partitioned_by_release_year(tmp_path):
    data = _seed(tmp_path)
    moment = datetime(2026, 10, 1, tzinfo=timezone.utc)
    publish_catalogue(data, run_at=moment)

    year = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    assert list(year["filing_id"]) == ["0000320193-26-000001"]
    assert "version" not in year.columns
    assert "ownership_pct" not in year.columns
    assert "valuation" not in year.columns
    row = year.iloc[0]
    assert row["form"] == "10-K"
    assert row["cik"] == "0000320193"
    assert row["archive_url"].startswith("https://www.sec.gov/Archives/edgar/data/")
    assert row["local_document_path"] == "bronze/sec/documents/cik=0000320193/000032019326000001/aapl-2026.htm"
    assert row["updated_date"] == "2026-10-01"
    schema = (data / "manifests" / "dev" / "schema.json").read_text(encoding="utf-8")
    assert "filing_id" in schema
    companies = pd.read_parquet(data / "manifests" / "dev" / "companies.parquet")
    joined = year.merge(companies, on="cik")
    assert joined.iloc[0]["ticker"] == "AAPL"


def test_correction_replaces_the_same_accession(tmp_path):
    data = _seed(tmp_path)
    first = datetime(2026, 10, 1, tzinfo=timezone.utc)
    second = datetime(2026, 10, 2, tzinfo=timezone.utc)
    publish_catalogue(data, run_at=first)
    filings_path = data / "silver" / "filings" / "cik=0000320193" / "filings.parquet"
    filings = pd.read_parquet(filings_path)
    filings.loc[filings["accession_number"].eq("0000320193-26-000001"), "form"] = "10-K/A"
    filings.to_parquet(filings_path, index=False)
    publish_catalogue(data, run_at=second)

    year = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    assert len(year) == 1
    assert year.iloc[0]["form"] == "10-K/A"
    assert year.iloc[0]["filing_id"] == "0000320193-26-000001"
    assert year.iloc[0]["updated_date"] == "2026-10-02"
    assert "version" not in year.columns
    changed = pd.read_parquet(data / "manifests" / "dev" / "incremental" / "2026-10-02.parquet")
    assert list(changed["filing_id"]) == ["0000320193-26-000001"]


def test_daily_incremental_is_the_watermark_and_unchanged_rows_keep_their_date(tmp_path):
    data = _seed(tmp_path)
    publish_catalogue(data, run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
    publish_catalogue(data, run_at=datetime(2026, 10, 2, tzinfo=timezone.utc))

    year = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    assert year.iloc[0]["updated_date"] == "2026-10-01"
    later = read_incremental_since(data, "2026-10-01")
    assert later.empty
    first_day = read_incremental_since(data, "2026-09-30")
    assert "0000320193-26-000001" in set(first_day["filing_id"])


def test_failed_accession_is_recorded_beside_the_incremental_file(tmp_path):
    data = _seed(tmp_path)
    cik = "0000320193"
    failed = "0000320193-26-000099"
    filings_path = data / "silver" / "filings" / f"cik={cik}" / "filings.parquet"
    filings = pd.read_parquet(filings_path)
    filings = pd.concat([
        filings,
        pd.DataFrame([_filing(cik, failed, "2026-04-01", "4", "form4.xml", "Apple Inc.")]),
    ], ignore_index=True)
    filings.to_parquet(filings_path, index=False)
    manifest_path = data / "bronze" / "sec" / "documents" / f"cik={cik}" / "document_manifest.parquet"
    manifest = pd.read_parquet(manifest_path)
    manifest = pd.concat([
        manifest,
        pd.DataFrame([_manifest(
            data, cik, failed, "2026-04-01", "4", "form4.xml",
            status="FAILED", error="ConnectionError: sec.gov",
        )]),
    ], ignore_index=True)
    manifest.to_parquet(manifest_path, index=False)

    publish_catalogue(data, run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
    failures = pd.read_parquet(data / "manifests" / "dev" / "incremental" / "2026-10-01.failures.parquet")
    row = failures[failures["filing_id"].eq(failed)].iloc[0]
    assert row["document_status"] == "FAILED"
    assert "ConnectionError" in row["error"]
    catalogue = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    failed_row = catalogue[catalogue["filing_id"].eq(failed)].iloc[0]
    assert failed_row["local_document_path"] is None or failed_row["local_document_path"] == ""
    assert failed_row["archive_url"].startswith("https://www.sec.gov/")


def test_replay_rebuilds_one_year_from_local_files(tmp_path):
    data = _seed(tmp_path)
    publish_catalogue(data, run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
    before = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    publish_catalogue(data, replay_year=2026, run_at=datetime(2026, 10, 3, tzinfo=timezone.utc))
    after = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    earlier = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2025" / "part-00000.parquet")
    assert list(after["filing_id"]) == list(before["filing_id"])
    assert after.iloc[0]["updated_date"] == "2026-10-01"
    assert list(earlier["filing_id"]) == ["0000320193-25-000001"]
    assert earlier.iloc[0]["updated_date"] == "2026-10-01"


def test_enroll_adds_the_master_row_and_only_that_companys_changes(tmp_path):
    data = _seed(tmp_path)
    publish_catalogue(data, run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
    cik = "0000789019"
    accession = "0000789019-26-000010"
    _write(
        data / "silver" / "filings" / f"cik={cik}" / "filings.parquet",
        pd.DataFrame([_filing(cik, accession, "2026-05-01", "8-K", "msft.htm", "Microsoft Corp")]),
    )
    _write(
        data / "bronze" / "sec" / "documents" / f"cik={cik}" / "document_manifest.parquet",
        pd.DataFrame([_manifest(data, cik, accession, "2026-05-01", "8-K", "msft.htm")]),
    )
    result = enroll_and_backfill(
        cik,
        company_name="Microsoft Corp",
        ticker="MSFT",
        data_root=data,
        run_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
    )
    assert result["master_added"] is True
    master = pd.read_parquet(data / "silver" / "company" / "company.parquet")
    assert cik in set(master["cik"].astype(str).str.zfill(10))
    registry = pd.read_parquet(data / "silver" / "platform" / "company_registry.parquet")
    assert cik in set(registry["cik"].astype(str).str.zfill(10))
    year = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    assert set(year["cik"]) == {"0000320193", cik}
    changed = read_incremental_since(data, "2026-10-01")
    assert set(changed["cik"]) == {cik}


def test_s3_sync_copies_the_manifest_tree_and_does_not_create_a_bucket(tmp_path):
    data = _seed(tmp_path)
    publish_catalogue(data, run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))

    class Client:
        def __init__(self):
            self.uploaded = []

        def head_bucket(self, Bucket):
            if Bucket == "missing":
                raise RuntimeError("Not Found")

        def upload_file(self, filename, bucket, key):
            self.uploaded.append(key)

    client = Client()
    result = sync_manifests_to_s3(data, "filings-bucket", "prod", client)
    assert result["objects"] == len(client.uploaded)
    assert "prod/schema.json" in client.uploaded
    assert "prod/companies.parquet" in client.uploaded
    assert "prod/filings/year=2026/part-00000.parquet" in client.uploaded
    assert "prod/incremental/2026-10-01.parquet" in client.uploaded
    missing = Client()
    with pytest.raises(RuntimeError):
        sync_manifests_to_s3(data, "missing", "prod", missing)
    assert missing.uploaded == []


def test_daily_run_isolates_failures_and_retries_only_the_failed_task(tmp_path):
    data = _seed(tmp_path)
    other = "0000789019"
    registry = pd.read_parquet(data / "silver" / "platform" / "company_registry.parquet")
    registry = pd.concat([
        registry,
        pd.DataFrame([_registry_row(other, "Microsoft Corp", "MSFT")]),
    ], ignore_index=True)
    registry.to_parquet(data / "silver" / "platform" / "company_registry.parquet", index=False)
    polls = {"count": 0}

    def poll(cik):
        polls["count"] += 1
        if cik == "0000320193":
            raise RuntimeError("sec down")
        return [{
            "accessionNumber": "0000789019-26-000099",
            "filingDate": "2026-06-01",
            "form": "8-K",
            "primaryDocument": "msft.htm",
            "companyName": "Microsoft Corp",
        }]

    def download(cik, filing):
        raise RuntimeError("dns")

    promotes = {"count": 0}

    def promote(data_root, env):
        promotes["count"] += 1
        if promotes["count"] == 1:
            raise RuntimeError("promote broke")
        return promote_local(data_root, env=env)

    moment = datetime(2026, 10, 11, 1, 30, tzinfo=timezone.utc)
    first = run_daily(
        data,
        env="dev",
        run_at=moment,
        run_id="daily-1",
        poll_recent=poll,
        download_accession=download,
        promote_fn=promote,
    )
    assert first["status"] == "FAILED"
    assert first["tasks"]["extract"]["status"] == "SUCCESS"
    assert first["tasks"]["catalogue"]["status"] == "SUCCESS"
    assert first["tasks"]["promote"]["status"] == "FAILED"
    assert polls["count"] == 2
    failures = pd.read_parquet(data / "manifests" / "dev" / "incremental" / "2026-10-11.failures.parquet")
    statuses = set(failures["document_status"])
    assert "POLL_FAILED" in statuses
    assert "EXTRACT_FAILED" in statuses
    catalogue = pd.read_parquet(data / "manifests" / "dev" / "filings" / "year=2026" / "part-00000.parquet")
    assert "0000320193-26-000001" in set(catalogue["filing_id"])
    assert "0000789019-26-000099" in set(catalogue["filing_id"])

    second = run_daily(
        data,
        env="dev",
        run_at=moment,
        run_id="daily-1",
        poll_recent=poll,
        download_accession=download,
        promote_fn=promote,
    )
    assert second["status"] == "SUCCESS"
    assert second["tasks"]["promote"]["status"] == "SUCCESS"
    assert polls["count"] == 2
    assert second["tasks"]["promote"]["valuation_status"] == "NO_DATA"
    assert not (data / "silver" / "valuation" / "company_valuations.parquet").exists()


def _quiet_promote(data_root, env):
    return {"valuation_status": "NO_DATA", "valuation_file_written": False}


def test_daily_refresh_skips_when_no_accession_is_new(tmp_path):
    data = _seed(tmp_path)
    calls = []

    def refresh(ciks):
        calls.append(list(ciks))

    result = run_daily(
        data,
        env="dev",
        run_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
        run_id="refresh-none",
        poll_recent=lambda cik: [],
        download_accession=lambda cik, filing: None,
        refresh_fn=refresh,
        promote_fn=_quiet_promote,
    )
    assert calls == []
    assert result["status"] == "SUCCESS"
    assert result["tasks"]["refresh"]["status"] == "SUCCESS"
    assert result["tasks"]["refresh"]["ciks"] == []
    assert result["tasks"]["promote"]["status"] == "SUCCESS"


def test_daily_refresh_passes_only_the_cik_that_downloaded(tmp_path):
    data = _seed(tmp_path)
    seen = []
    order = []

    def refresh(ciks):
        order.append("refresh")
        seen.append(list(ciks))
        return {"ciks": list(ciks)}

    def promote(data_root, env):
        order.append("promote")
        return _quiet_promote(data_root, env)

    result = run_daily(
        data,
        env="dev",
        run_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
        run_id="refresh-one",
        poll_recent=lambda cik: [{
            "accessionNumber": "0000320193-26-000099",
            "filingDate": "2026-06-01",
            "form": "8-K",
            "primaryDocument": "new.htm",
        }],
        download_accession=lambda cik, filing: None,
        refresh_fn=refresh,
        promote_fn=promote,
    )
    assert seen == [["0000320193"]]
    assert order == ["refresh", "promote"]
    assert result["tasks"]["refresh"]["ciks"] == ["0000320193"]
    assert result["tasks"]["extract"]["downloaded_ciks"] == ["0000320193"]
    assert result["status"] == "SUCCESS"


def test_daily_refresh_failure_is_isolated_and_promote_still_runs(tmp_path):
    data = _seed(tmp_path)
    other = "0000789019"
    registry = pd.read_parquet(data / "silver" / "platform" / "company_registry.parquet")
    registry = pd.concat([
        registry,
        pd.DataFrame([_registry_row(other, "Microsoft Corp", "MSFT")]),
    ], ignore_index=True)
    registry.to_parquet(data / "silver" / "platform" / "company_registry.parquet", index=False)
    order = []

    def poll(cik):
        return [{
            "accessionNumber": f"{cik}-26-000099",
            "filingDate": "2026-06-01",
            "form": "8-K",
            "primaryDocument": "new.htm",
            "companyName": "Example",
        }]

    def refresh(ciks):
        order.append(ciks[0])
        if ciks == ["0000320193"]:
            raise RuntimeError("refresh broke")
        return {"ciks": list(ciks)}

    def promote(data_root, env):
        order.append("promote")
        return _quiet_promote(data_root, env)

    result = run_daily(
        data,
        env="dev",
        run_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
        run_id="refresh-fail",
        poll_recent=poll,
        download_accession=lambda cik, filing: None,
        refresh_fn=refresh,
        promote_fn=promote,
    )
    assert result["status"] == "SUCCESS"
    assert order == ["0000320193", "0000789019", "promote"]
    failures = result["tasks"]["refresh"]["company_failures"]
    assert failures[0]["cik"] == "0000320193"
    assert failures[0]["document_status"] == "REFRESH_FAILED"
    assert "refresh broke" in failures[0]["error"]
    assert result["tasks"]["refresh"]["ciks"] == ["0000789019"]
    assert result["tasks"]["promote"]["status"] == "SUCCESS"
    assert result["tasks"]["promote"]["valuation_status"] == "NO_DATA"


def test_daily_refresh_keeps_the_pipeline_error_and_retries_the_cik(tmp_path):
    data = _seed(tmp_path)
    calls = []
    pipeline_error = "ImportError: HTML extraction requires the beautifulsoup4 package"

    def refresh(ciks):
        calls.append(list(ciks))
        if len(calls) == 1:
            return {
                "company_failures": 1,
                "runs": [{
                    "run_status": "FAILED",
                    "error": pipeline_error,
                    "run_id": "pipeline-1",
                }],
            }
        return {"ciks": list(ciks), "company_failures": 0, "runs": []}

    def new_filing(cik):
        return [{
            "accessionNumber": "0000320193-26-000099",
            "filingDate": "2026-06-01",
            "form": "8-K",
            "primaryDocument": "new.htm",
        }]

    first = run_daily(
        data,
        env="dev",
        run_at=datetime(2026, 10, 13, tzinfo=timezone.utc),
        run_id="2026-10-13T145042Z",
        poll_recent=new_filing,
        download_accession=lambda cik, filing: None,
        refresh_fn=refresh,
        promote_fn=_quiet_promote,
    )
    stored = first["tasks"]["refresh"]["company_failures"][0]["error"]
    assert "FAILED" in stored
    assert pipeline_error in stored
    assert "run_id=pipeline-1" in stored
    assert "company refresh failure" not in stored

    second = run_daily(
        data,
        env="dev",
        run_at=datetime(2026, 10, 14, tzinfo=timezone.utc),
        run_id="2026-10-14T145042Z",
        poll_recent=lambda cik: [],
        download_accession=lambda cik, filing: None,
        refresh_fn=refresh,
        promote_fn=_quiet_promote,
    )
    assert calls == [["0000320193"], ["0000320193"]]
    assert second["tasks"]["extract"]["downloaded_ciks"] == []
    assert second["tasks"]["refresh"]["ciks"] == ["0000320193"]
    assert second["tasks"]["refresh"]["company_failures"] == []
    assert second["tasks"]["promote"]["valuation_status"] == "NO_DATA"


def test_prod_env_is_a_separate_tree(tmp_path):
    data = _seed(tmp_path)
    publish_catalogue(data, env="prod", run_at=datetime(2026, 10, 1, tzinfo=timezone.utc))
    assert (data / "manifests" / "prod" / "companies.parquet").exists()
    assert not (data / "manifests" / "dev" / "companies.parquet").exists()


def test_replay_rebuilds_lineaged_financials_and_holders_for_the_year(tmp_path):
    data = _seed(tmp_path)
    _write(
        data / "gold" / "financials" / "cik=0000320193" / "financials.parquet",
        pd.DataFrame([
            {"cik": "0000320193", "metric": "revenue", "filing_date": "2026-02-01", "period_end": "2025-12-31", "accession_number": "0000320193-26-000001", "value": 10},
            {"cik": "0000320193", "metric": "revenue", "filing_date": "2025-02-01", "period_end": "2024-12-31", "accession_number": "0000320193-25-000001", "value": 9},
            {"cik": "0000320193", "metric": "revenue", "filing_date": None, "period_end": None, "accession_number": None, "value": 1},
        ]),
    )
    _write(
        data / "silver" / "funding" / "investors.parquet",
        pd.DataFrame([
            {"cik": "0000320193", "investor_name": "Vanguard", "investor_type": "UNKNOWN", "ownership_pct": None, "source_accession_number": "0000320193-26-000008", "as_of_date": "2026-03-01", "investor_source": "SEC_SCHEDULE_13G"},
            {"cik": "0000320193", "investor_name": "Nobody", "investor_type": "UNKNOWN", "ownership_pct": 4.0, "source_accession_number": None, "as_of_date": None, "investor_source": "SEC_SCHEDULE_13G"},
        ]),
    )
    publish_catalogue(data, replay_year=2026, run_at=datetime(2026, 10, 3, tzinfo=timezone.utc))
    financials = pd.read_parquet(data / "manifests" / "dev" / "promoted" / "financials" / "year=2026" / "part-00000.parquet")
    holders = pd.read_parquet(data / "manifests" / "dev" / "promoted" / "holders" / "year=2026" / "part-00000.parquet")
    assert list(financials["value"]) == [10]
    assert list(holders["investor_name"]) == ["Vanguard"]
    assert holders.iloc[0]["ownership_pct"] is None or pd.isna(holders.iloc[0]["ownership_pct"])
    assert not (data / "manifests" / "dev" / "promoted" / "financials" / "year=2025").exists()
    assert not (data / "silver" / "valuation").exists()


def test_gold_lineage_blocks_rows_with_no_accession_and_no_as_of(tmp_path):
    kept = retain_lineaged_rows(pd.DataFrame([
        {"accession_number": "0000320193-26-000001", "filing_date": None, "ownership_pct": None},
        {"accession_number": None, "filing_date": None, "period_end": None, "ownership_pct": 9},
    ]))
    assert len(kept) == 1
    assert kept.iloc[0]["accession_number"] == "0000320193-26-000001"
    assert kept.iloc[0]["ownership_pct"] is None or pd.isna(kept.iloc[0]["ownership_pct"])

    events = build_investor_events(
        pd.DataFrame(),
        pd.DataFrame([
            {"cik": "0000320193", "investor_name": "Vanguard", "investor_type": "UNKNOWN", "investor_source": "SEC_SCHEDULE_13G", "source_accession_number": "0000320193-26-000008", "as_of_date": "2026-03-01", "ownership_pct": None},
            {"cik": "0000320193", "investor_name": "Nobody", "investor_type": "UNKNOWN", "investor_source": "SEC_SCHEDULE_13G", "source_accession_number": None, "as_of_date": None, "ownership_pct": 4.0},
        ]),
        "SEC_FORM_D_DATASET",
    )
    assert list(events["investor_name"]) == ["Vanguard"]

    cik = "0000320193"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    frame = pd.DataFrame([
        {"cik": cik, "metric": "revenue", "unit": "USD", "period_type": "annual", "form": "10-K", "fiscal_year": 2026, "period_start": "2025-01-01", "period_end": "2025-12-31", "filing_date": "2026-02-01", "accession_number": "0000320193-26-000001", "value": 10},
        {"cik": cik, "metric": "revenue", "unit": "USD", "period_type": "annual", "form": "10-K", "fiscal_year": 2024, "period_start": None, "period_end": None, "filing_date": None, "accession_number": None, "value": 99},
    ])
    _write(silver / "financial_facts" / f"cik={cik}" / "financial_facts.parquet", frame)
    canonical = canonicalize_company(silver, gold, cik)
    assert list(canonical["value"]) == [10]
