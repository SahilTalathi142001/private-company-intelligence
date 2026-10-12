from pathlib import Path
WEB=Path(__file__).resolve().parents[2]/"src"/"company_platform"/"web"
def test_step34_assets_exist():
    assert (WEB/"app.js").exists() and (WEB/"styles.css").exists()
def test_financial_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["Financial Intelligence","financial_score","Financial health","Risk signal","Trajectory"]: assert v in x
def test_funding_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["Funding & Capital Activity","form_d_observations","Latest offering","SEC Form D"]: assert v in x
    assert "not a complete history of company fundraising" in x
def test_investment_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["Investment Signals","investment_signal_count","Capital activity","Recurring activity","Capital raise execution"]: assert v in x
    assert "NO FUNDING SIGNALS" in x
def test_state_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "Company State" in x and "State status" in x and "NO_DATA" in x
def test_lineage_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["Traceability","Source event ID","Source run ID","Source document ID"]: assert v in x
def test_data_coverage_layer():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    for v in ["Source-to-Website Audit","Form D coverage","Form D observations","Production structured events","Test events excluded"]: assert v in x
def test_no_frontend_scoring_logic():
    x=(WEB/"app.js").read_text(encoding="utf-8")
    assert "overall_score =" not in x and "calculateScore" not in x
def test_responsive():
    x=(WEB/"styles.css").read_text(encoding="utf-8")
    assert "@media(max-width:900px)" in x
    assert "@media(max-width:560px)" in x
