from sec_ingestion.beneficial_ownership import (
    _proxy_holders,
    _reporting_persons,
)


def test_schedule_13g_cover_states_name_and_percent():
    text = """
    1
    Names of Reporting Persons
    Vanguard Capital Management
    11
    Percent of class represented by amount in row (9)
    7.48 %
    """
    holders = _reporting_persons(text)
    assert holders == [{
        "investor_name": "Vanguard Capital Management",
        "ownership_pct": 7.48,
    }]


def test_proxy_table_states_holder_percent_and_skips_group_total():
    text = """
    Security Ownership of Certain Beneficial Owners and Management
    The Vanguard Group  1,234,567  8.4%
    All directors and executive officers as a group  2,000,000  1.1%
    Certain Relationships and Related Transactions
    """
    holders = _proxy_holders(text, "Apple Inc.")
    assert holders == [{"investor_name": "The Vanguard Group", "ownership_pct": 8.4}]


def test_proxy_table_reads_percent_on_the_following_line():
    text = """
    Security Ownership of Certain Beneficial Owners and Management
    beneficial owners of more than 5% of our common stock
    The Vanguard Group
    142,442,745
    8.8%
    Lisa T. Su
    4,305,973
    *
    All current directors and executive officers as a group (16 persons)
    8,107,566
    *
    """
    holders = _proxy_holders(text, "Advanced Micro Devices Inc")
    by_name = {row["investor_name"]: row["ownership_pct"] for row in holders}
    assert by_name["The Vanguard Group"] == 8.8
    assert by_name["Lisa T. Su"] is None
    assert not any("as a group" in name.lower() for name in by_name)
