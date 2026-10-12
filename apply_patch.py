from pathlib import Path

target = Path("src/company_platform/integration_v2/components.py")
text = target.read_text(encoding="utf-8")

if "'company_discovery': _a8_adapter(_company_discovery)" in text:
    print("company_discovery handler already exists; no changes made.")
    raise SystemExit(0)

anchor = "\nHANDLERS: dict[str, Callable] = {"
if anchor not in text:
    raise SystemExit("Could not find HANDLERS anchor. No changes made.")



handler = r