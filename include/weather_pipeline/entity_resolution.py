"""Entity resolution and ownership attribution for site inventory records.

Real inventories are messy: "Phoenix, AZ", "PHX", "Londn", "São Paulo", "Delhi NCR".
This module maps each raw location string to a canonical city, and records HOW it
was matched (exact / alias / fuzzy) and how confident the match is, so every
decision is explainable and auditable. Records that cannot be matched safely are
flagged rather than guessed.

Ownership (business unit, owner team, criticality tier) is assigned by
declarative rules in YAML, so changes go through code review like any other code.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import yaml
from rapidfuzz import fuzz, process

from weather_pipeline import config

# Only commas and spaced hyphens separate a place from a suffix ("Phoenix, AZ",
# "Sao Paulo - BR"). Slashes are NOT split, so "Tokyo / London" stays ambiguous.
_SUFFIX_SPLIT = re.compile(r"\s*,\s*|\s+-\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize(raw: str) -> str:
    """'São Paulo, BR' -> 'sao paulo'. Drops suffixes, accents, case and punctuation."""
    place = _SUFFIX_SPLIT.split(raw.strip(), maxsplit=1)[0]
    decomposed = unicodedata.normalize("NFKD", place)
    without_accents = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return _NON_ALNUM.sub(" ", without_accents.lower()).strip()


@dataclass(frozen=True)
class Resolution:
    raw: str
    city: str | None  # canonical city name, or None when not safely resolved
    method: str  # exact | alias | fuzzy | none
    score: float  # 0-100
    status: str  # resolved | ambiguous | unresolved


class CityResolver:
    def __init__(
        self,
        cities: list[str],
        aliases: dict[str, list[str]] | None = None,
        threshold: float = config.FUZZY_THRESHOLD,
        ambiguity_margin: float = config.AMBIGUITY_MARGIN,
    ) -> None:
        self.threshold = threshold
        self.ambiguity_margin = ambiguity_margin
        self._canonical = {normalize(c): c for c in cities}
        self._choices = list(self._canonical)
        self._aliases: dict[str, str] = {}
        for city, names in (aliases or {}).items():
            if city not in cities:
                raise ValueError(f"Alias table references unknown city '{city}'")
            for name in names:
                key = normalize(name)
                if self._aliases.get(key, city) != city:
                    raise ValueError(f"Alias '{name}' maps to more than one city")
                self._aliases[key] = city

    def resolve(self, raw: str | None) -> Resolution:
        raw = raw or ""
        key = normalize(raw)
        if not key:
            return Resolution(raw, None, "none", 0.0, "unresolved")
        if key in self._canonical:
            return Resolution(raw, self._canonical[key], "exact", 100.0, "resolved")
        if key in self._aliases:
            return Resolution(raw, self._aliases[key], "alias", 100.0, "resolved")

        ranked = process.extract(key, self._choices, scorer=fuzz.token_set_ratio, limit=2)
        best_key, best_score, _ = ranked[0]
        if best_score < self.threshold:
            return Resolution(raw, None, "none", round(best_score, 1), "unresolved")
        if len(ranked) > 1 and best_score - ranked[1][1] < self.ambiguity_margin:
            return Resolution(raw, None, "fuzzy", round(best_score, 1), "ambiguous")
        return Resolution(raw, self._canonical[best_key], "fuzzy", round(best_score, 1), "resolved")


def _matches(when: dict, site: dict) -> bool:
    for condition, expected in when.items():
        if condition == "site_type":
            ok = site.get("site_type") == expected
        elif condition == "cost_center_prefix":
            ok = str(site.get("cost_center", "")).startswith(expected)
        else:
            raise ValueError(f"Unknown rule condition '{condition}'")
        if not ok:
            return False
    return True


def assign_ownership(site: dict, rules_doc: dict) -> dict:
    """Apply YAML rules. First rule to set a field wins; defaults fill the rest."""
    assigned: dict = {}
    matched: list[str] = []
    for rule in rules_doc.get("rules", []):
        if _matches(rule.get("when", {}), site):
            matched.append(rule["name"])
            for field, value in rule.get("then", {}).items():
                assigned.setdefault(field, value)
    for field, value in rules_doc.get("defaults", {}).items():
        assigned.setdefault(field, value)
    assigned["matched_rules"] = ",".join(matched)
    return assigned


def load_inventory(path: Path | None = None) -> list[dict]:
    path = path or config.REFERENCE_DIR / "site_inventory.csv"
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_yaml(name: str) -> dict:
    with open(config.REFERENCE_DIR / name, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def resolve_inventory(
    inventory: list[dict], cities: list[str], aliases: dict, rules_doc: dict
) -> list[dict]:
    """Resolve every site to a city and attach ownership. Returns warehouse-ready records."""
    resolver = CityResolver(cities, aliases)
    records = []
    for site in inventory:
        res = resolver.resolve(site["raw_location"])
        ownership = assign_ownership(site, rules_doc)
        records.append(
            {
                "site_id": site["site_id"],
                "raw_location": site["raw_location"],
                "site_type": site["site_type"],
                "cost_center": site["cost_center"],
                "resolved_city": res.city,
                "match_method": res.method,
                "match_score": res.score,
                "status": res.status,
                "business_unit": ownership["business_unit"],
                "owner_team": ownership["owner_team"],
                "criticality_tier": ownership["criticality_tier"],
                "matched_rules": ownership["matched_rules"],
            }
        )
    return records
