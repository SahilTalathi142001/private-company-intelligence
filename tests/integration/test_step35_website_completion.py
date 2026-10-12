from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
WEB=ROOT/'src'/'company_platform'/'web'

def test_assets_exist():
    for n in ('index.html','app.js','styles.css'): assert (WEB/n).exists()

def test_no_frontend_scoring_logic():
    js=(WEB/'app.js').read_text(encoding='utf-8')
    assert 'sort(' not in js
    assert 'financial_score =' not in js
    assert 'calculate' not in js.lower()

def test_required_views():
    js=(WEB/'app.js').read_text(encoding='utf-8')
    for x in ('dashboard','coveragePage','operations','eventList','lineage','Source-to-Website Audit'):
        assert x in js

def test_responsive_css():
    css=(WEB/'styles.css').read_text(encoding='utf-8')
    assert '@media' in css

def test_api_contract():
    app=(ROOT/'src'/'company_platform'/'api'/'app.py').read_text(encoding='utf-8')
    for x in ('/api/companies','/api/events/{event_id}','/api/runs/{run_id}','/api/operations/summary'):
        assert x in app
