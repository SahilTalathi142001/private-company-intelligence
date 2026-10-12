from pathlib import Path
WEB=Path(__file__).resolve().parents[2]/"src"/"company_platform"/"web"
def test_assets_exist(): assert all((WEB/x).exists() for x in ["index.html","app.js","styles.css"])
def test_index_assets():
    x=(WEB/"index.html").read_text(encoding="utf-8")
    assert "/web/styles.css" in x and "/web/app.js" in x
def test_core_routes():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["/api/companies","/api/operations/summary","/api/events/"]: assert v in x
def test_api_boundary():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "async function api(url)" in x and "fetch(url)" in x
    assert "if(!r.ok)throw new Error" in x
def test_states():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "Unable to load this view" in x and "Retry" in x
def test_identity():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "company_id" in x and "cik" in x
def test_operations_view_uses_api_summary():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "async function operations()" in x
    assert "api('/api/operations/summary')" in x
def test_responsive():
    x=(WEB/"styles.css").read_text(encoding="utf-8")
    assert "@media(max-width:900px)" in x
    assert "@media(max-width:560px)" in x
def test_patch_mounts_site_and_api():
    from company_platform.api.app import app

    routes = {route.path for route in app.routes}
    assert "/web" in routes and "/" in routes and "/api/companies" in routes
