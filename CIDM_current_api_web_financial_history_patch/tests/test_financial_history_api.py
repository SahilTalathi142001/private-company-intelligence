import unittest
from unittest.mock import patch
import pandas as pd

from company_platform.api import service


class FinancialHistoryTests(unittest.TestCase):
    def setUp(self):
        self.frame = pd.DataFrame([
            {"cik": "0000320193", "metric": "Revenue", "value": 100.0, "unit": "USD", "period_start": "2024-01-01", "period_end": "2024-12-31", "fiscal_year": 2024, "fiscal_period": "FY", "filing_date": "2025-02-01"},
            {"cik": "0000320193", "metric": "Assets", "value": None, "unit": "USD", "period_start": None, "period_end": "2024-12-31", "fiscal_year": 2024, "fiscal_period": "FY", "filing_date": "2025-02-01"},
            {"cik": "0000320193", "metric": "Revenue", "value": 80.0, "unit": "USD", "period_start": "2023-01-01", "period_end": "2023-12-31", "fiscal_year": 2023, "fiscal_period": "FY", "filing_date": "2024-02-01"},
            {"cik": "0000789019", "metric": "Revenue", "value": 999.0, "unit": "USD", "period_start": "2024-01-01", "period_end": "2024-12-31", "fiscal_year": 2024, "fiscal_period": "FY", "filing_date": "2025-02-01"},
        ])

    def call(self, **kwargs):
        with patch.object(service, "_record_by_cik", return_value={"cik": "0000320193"}), \
             patch.object(service, "_first_frame", return_value=(self.frame.copy(), "gold/financials/cik=0000320193/financials.parquet")):
            return service.get_financial_history("0000320193", **kwargs)

    def test_filters_to_requested_cik(self):
        result = self.call(limit=50, offset=0)
        self.assertEqual(result["cik"], "0000320193")
        self.assertEqual(result["total"], 3)
        self.assertTrue(all(row["cik"] == "0000320193" for row in result["items"]))

    def test_metric_and_year_filters(self):
        result = self.call(metric="revenue", fiscal_year=2024, limit=50, offset=0)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["value"], 100.0)
        self.assertEqual(result["filters"]["available_fiscal_years"], [2023, 2024])

    def test_missing_values_remain_missing(self):
        result = self.call(metric="assets", limit=50, offset=0)
        self.assertEqual(result["total"], 1)
        self.assertIsNone(result["items"][0]["value"])

    def test_pagination(self):
        result = self.call(limit=1, offset=1, sort="fiscal_year", direction="asc")
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["total"], 3)

    def test_unregistered_company_returns_none(self):
        with patch.object(service, "_record_by_cik", return_value=None):
            self.assertIsNone(service.get_financial_history("0000000000"))


if __name__ == "__main__":
    unittest.main()
