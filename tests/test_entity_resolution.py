import pytest
from weather_pipeline import entity_resolution as er

CITIES = ["phoenix", "london", "delhi", "sao_paulo", "tokyo", "nairobi"]
ALIASES = er.load_yaml("city_aliases.yaml")
RULES = er.load_yaml("ownership_rules.yaml")


@pytest.fixture(scope="module")
def resolver():
    return er.CityResolver(CITIES, ALIASES)


@pytest.mark.parametrize(
    ("raw", "city", "method"),
    [
        ("Phoenix, AZ", "phoenix", "exact"),
        ("São Paulo", "sao_paulo", "exact"),
        ("Sao-Paulo - BR", "sao_paulo", "exact"),
        ("PHX", "phoenix", "alias"),
        ("Tokio", "tokyo", "alias"),
        ("New Delhi", "delhi", "alias"),
        ("Londn", "london", "fuzzy"),
        ("Delhi NCR", "delhi", "fuzzy"),
        ("Nairobi Kenya", "nairobi", "fuzzy"),
    ],
)
def test_resolves_messy_names(resolver, raw, city, method):
    res = resolver.resolve(raw)
    assert (res.city, res.method, res.status) == (city, method, "resolved")


def test_unknown_location_is_unresolved_not_guessed(resolver):
    res = resolver.resolve("Atlantis Lab")
    assert res.city is None and res.status == "unresolved"


def test_two_cities_in_one_string_is_ambiguous(resolver):
    res = resolver.resolve("Tokyo London Joint Lab")
    assert res.city is None and res.status == "ambiguous"


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_empty_input_is_unresolved(resolver, raw):
    assert resolver.resolve(raw).status == "unresolved"


def test_conflicting_alias_is_rejected():
    with pytest.raises(ValueError):
        er.CityResolver(CITIES, {"london": ["xx"], "tokyo": ["xx"]})


def test_alias_for_unknown_city_is_rejected():
    with pytest.raises(ValueError):
        er.CityResolver(CITIES, {"atlantis": ["atl"]})


def test_ownership_first_rule_wins_and_defaults_fill():
    site = {"site_type": "data_center", "cost_center": "CC-SEC-100"}
    out = er.assign_ownership(site, RULES)
    assert out["criticality_tier"] == 1
    assert (out["business_unit"], out["owner_team"]) == ("Security", "secops")
    assert out["matched_rules"] == "data-centers-are-tier-1,security-cost-centers"

    unknown = er.assign_ownership({"site_type": "lab", "cost_center": "CC-XYZ-1"}, RULES)
    assert unknown["owner_team"] == "unassigned" and unknown["criticality_tier"] == 3


def test_unknown_rule_condition_raises():
    bad = {"rules": [{"name": "x", "when": {"colour": "red"}, "then": {"owner_team": "a"}}]}
    with pytest.raises(ValueError):
        er.assign_ownership({"site_type": "lab"}, bad)


def test_bundled_inventory_resolution_summary():
    records = er.resolve_inventory(er.load_inventory(), CITIES, ALIASES, RULES)
    by_status = {}
    for r in records:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    assert by_status == {"resolved": 11, "unresolved": 1, "ambiguous": 1}
    assert all(r["owner_team"] != "unassigned" for r in records)
