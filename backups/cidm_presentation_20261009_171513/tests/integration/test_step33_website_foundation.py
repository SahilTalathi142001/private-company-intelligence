from pathlib import Path

WEB = Path(__file__).resolve().parents[2] / "src" / "company_platform" / "web"


def test_assets_exist():
    assert all((WEB / name).exists() for name in ["index.html", "app.js", "styles.css", "assets/app.js", "assets/styles.css"])


def test_index_uses_replacement_assets():
    html = (WEB / "index.html").read_text(encoding="utf-8")
    assert "/assets/styles.css" in html and "/assets/app.js" in html
    assert "Private Company Intelligence" in html


def test_frontend_uses_versioned_api_contract():
    js = (WEB / "assets/app.js").read_text(encoding="utf-8")
    for route in ["/api/v1/companies", "/api/v1/coverage", "/api/v1/operations/summary"]:
        assert route in js

def test_api_boundary_handles_errors():
    js = (WEB / "assets/app.js").read_text(encoding="utf-8")
    assert "fetch(url" in js and "if(!res.ok) throw new Error" in js

def test_error_view_does_not_invent_fallback_data():
    js = (WEB / "assets/app.js").read_text(encoding="utf-8")
    assert "No fallback company or synthetic metrics were substituted" in js
    assert "Try again" in js

def test_identity_and_data_domains_are_explicit():
    js = (WEB / "assets/app.js").read_text(encoding="utf-8")
    assert "company_id" in js and "cik" in js
    for domain in ["financials", "funding", "ownership", "valuation", "investment_signals"]:
        assert domain in js

def test_operations_view_uses_read_only_summary():
    js = (WEB / "assets/app.js").read_text(encoding="utf-8")
    assert "async function renderOperations()" in js
    assert "api('/api/v1/operations/summary')" in js

def test_responsive_layout():
    css = (WEB / "assets/styles.css").read_text(encoding="utf-8")
    assert "@media(max-width:1200px)" in css and "@media(max-width:760px)" in css

def test_compatibility_routes_keep_step34_smoke_check_working():
    from company_platform.api.app import app
    routes = {route.path for route in app.routes}
    assert "/web" in routes and "/api/companies" in routes
    assert "/api/v1/companies" in routes
