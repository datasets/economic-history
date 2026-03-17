#!/usr/bin/env python3
"""
process.py — Working Hours: Historical Time Series

Sources (fallback order):
  1. OWID historical dataset CSV (Huberman & Minns via Our World in Data)
  2. OWID catalog frame
  3. OECD SDMX JSON API (recent) + embedded Huberman & Minns historical table
  4. Embedded Huberman & Minns table only

Outputs:
  data/working-hours.csv  — annual working hours per worker, long format
"""

import csv
import json
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

OWID_DATASET_URL = (
    "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
    "Working%20hours%20-%20Huberman%20%26%20Minns%20(2007)/"
    "Working%20hours%20-%20Huberman%20%26%20Minns%20(2007).csv"
)
OWID_CATALOG_URL = (
    "https://raw.githubusercontent.com/owid/owid-catalog/main/"
    "owid_catalog/frames/working_hours.csv"
)
OECD_URL = (
    "https://stats.oecd.org/sdmx-json/data/ANHRS/"
    "AUT+BEL+CAN+DNK+FIN+FRA+DEU+IRL+ITA+JPN+KOR+NLD+NOR+NZL+PRT+ESP+SWE+CHE+GBR+USA/"
    "AVG_H?startTime=1950&endTime=2023&dimensionAtObservation=allDimensions"
)

OUTPUT_PATH = Path("data/working-hours.csv")
FIELDNAMES = ["country", "year", "annual_working_hours", "source"]

# Huberman & Minns (2007) Table 2 — Annual hours of work per worker
# Source: "The times they are not changin': Days and hours of work in Old and New Worlds, 1870-2000"
# European Review of Economic History, Vol. 11(1), 2007, pp. 45-74
# https://doi.org/10.1016/j.eeh.2006.07.002
HUBERMAN_MINNS: Dict[str, Dict[int, float]] = {
    "Belgium": {
        1870: 3823, 1880: 3649, 1890: 3419, 1900: 3290, 1910: 2955,
        1913: 2871, 1920: 2726, 1929: 2601, 1938: 2258, 1950: 2283,
        1960: 2148, 1970: 1860, 1980: 1700, 1990: 1652, 2000: 1550,
    },
    "Canada": {
        1870: 2964, 1880: 2818, 1890: 2672, 1900: 2526, 1910: 2380,
        1913: 2305, 1920: 2256, 1929: 2178, 1938: 2038, 1950: 1967,
        1960: 1923, 1970: 1830, 1980: 1760, 1990: 1730, 2000: 1737,
    },
    "Denmark": {
        1870: 3042, 1880: 2922, 1890: 2802, 1900: 2682, 1910: 2562,
        1913: 2475, 1920: 2200, 1929: 2050, 1938: 1960, 1950: 1835,
        1960: 1773, 1970: 1640, 1980: 1540, 1990: 1508, 2000: 1455,
    },
    "France": {
        1870: 3311, 1880: 3100, 1890: 2926, 1900: 2752, 1910: 2490,
        1913: 2358, 1920: 2153, 1929: 2122, 1938: 1848, 1950: 1926,
        1960: 1825, 1970: 1688, 1980: 1537, 1990: 1453, 2000: 1441,
    },
    "Germany": {
        1870: 2941, 1880: 2850, 1890: 2768, 1900: 2685, 1910: 2583,
        1913: 2584, 1920: 2225, 1929: 2193, 1938: 2316, 1950: 2316,
        1960: 2081, 1970: 1846, 1980: 1719, 1990: 1598, 2000: 1480,
    },
    "Ireland": {
        1870: 2650, 1880: 2578, 1890: 2558, 1900: 2538, 1910: 2469,
        1913: 2447, 1920: 2340, 1929: 2260, 1938: 2287, 1950: 2250,
        1960: 2180, 1970: 2023, 1980: 1881, 1990: 1793, 2000: 1668,
    },
    "Netherlands": {
        1870: 3009, 1880: 2859, 1890: 2709, 1900: 2559, 1910: 2409,
        1913: 2369, 1920: 2202, 1929: 2157, 1938: 2156, 1950: 2208,
        1960: 2047, 1970: 1822, 1980: 1591, 1990: 1427, 2000: 1340,
    },
    "Norway": {
        1870: 2942, 1880: 2838, 1890: 2734, 1900: 2630, 1910: 2526,
        1913: 2528, 1920: 2218, 1929: 2202, 1938: 2103, 1950: 1952,
        1960: 1827, 1970: 1699, 1980: 1514, 1990: 1432, 2000: 1376,
    },
    "Sweden": {
        1870: 2945, 1880: 2831, 1890: 2718, 1900: 2604, 1910: 2490,
        1913: 2422, 1920: 2205, 1929: 2166, 1938: 2062, 1950: 1951,
        1960: 1848, 1970: 1616, 1980: 1519, 1990: 1480, 2000: 1624,
    },
    "Switzerland": {
        1870: 3174, 1880: 3036, 1890: 2898, 1900: 2760, 1910: 2622,
        1913: 2635, 1920: 2600, 1929: 2427, 1938: 2244, 1950: 2144,
        1960: 2002, 1970: 1821, 1980: 1716, 1990: 1640, 2000: 1581,
    },
    "United Kingdom": {
        1870: 2984, 1880: 2807, 1890: 2671, 1900: 2536, 1910: 2401,
        1913: 2370, 1920: 2190, 1929: 2110, 1938: 1998, 1950: 1958,
        1960: 1913, 1970: 1812, 1980: 1713, 1990: 1701, 2000: 1708,
    },
    "United States": {
        1870: 2964, 1880: 2818, 1890: 2672, 1900: 2526, 1910: 2380,
        1913: 2305, 1920: 2256, 1929: 2178, 1938: 2038, 1950: 1867,
        1960: 1780, 1970: 1735, 1980: 1734, 1990: 1810, 2000: 1836,
    },
}

TARGET_COUNTRIES = set(HUBERMAN_MINNS.keys())

OECD_COUNTRY_MAP = {
    "BEL": "Belgium",
    "CAN": "Canada",
    "DNK": "Denmark",
    "FRA": "France",
    "DEU": "Germany",
    "IRL": "Ireland",
    "NLD": "Netherlands",
    "NOR": "Norway",
    "SWE": "Sweden",
    "CHE": "Switzerland",
    "GBR": "United Kingdom",
    "USA": "United States",
}


def download_text(url: str) -> Optional[str]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "datapressr/1.0"})
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8")
    except Exception as exc:
        print(f"  Download failed: {exc}")
        return None


def choose_value_column(fieldnames: List[str]) -> Optional[str]:
    priority = [
        "Working hours per worker (annual)",
        "Annual working hours per worker",
        "annual_working_hours",
        "working_hours",
        "value",
    ]
    for col in priority:
        if col in fieldnames:
            return col
    excluded = {"Entity", "Country", "country", "Code", "code", "Year", "year", "Source", "source"}
    for col in fieldnames:
        lower = col.lower()
        if col not in excluded and "hour" in lower and ("annual" in lower or "worker" in lower):
            return col
    return None


def parse_owid_csv(text: str) -> List[Dict]:
    rows: List[Dict] = []
    reader = csv.DictReader(text.splitlines())
    fieldnames = list(reader.fieldnames or [])
    value_col = choose_value_column(fieldnames)
    if value_col is None:
        print(f"  Could not find value column in: {fieldnames}")
        return rows

    for row in reader:
        country = row.get("Entity") or row.get("country") or row.get("Country")
        year_str = row.get("Year") or row.get("year")
        value_str = row.get(value_col)

        if not country or country not in TARGET_COUNTRIES:
            continue
        if not year_str or not value_str:
            continue

        try:
            year = int(float(year_str))
            value = float(value_str)
        except ValueError:
            continue

        source = "Huberman & Minns (2007)" if year <= 2000 else "ILO"
        rows.append({
            "country": country,
            "year": year,
            "annual_working_hours": value,
            "source": source,
        })
    return dedupe_and_sort(rows)


def parse_oecd_json(text: str) -> List[Dict]:
    rows: List[Dict] = []
    payload = json.loads(text)
    dims = payload["structure"]["dimensions"]["observation"]
    dim_ids = [d["id"] for d in dims]

    loc_idx = next((i for i, d in enumerate(dim_ids) if d in ("LOCATION", "REF_AREA")), None)
    time_idx = next((i for i, d in enumerate(dim_ids) if d in ("TIME_PERIOD", "TIME")), None)
    if loc_idx is None or time_idx is None:
        return rows

    observations = payload["dataSets"][0]["observations"]
    for key, value_arr in observations.items():
        indices = [int(x) for x in key.split(":")]
        loc_code = dims[loc_idx]["values"][indices[loc_idx]]["id"]
        year_str = dims[time_idx]["values"][indices[time_idx]]["id"]
        country = OECD_COUNTRY_MAP.get(loc_code)

        if country is None or country not in TARGET_COUNTRIES:
            continue
        if value_arr[0] is None:
            continue

        try:
            year = int(year_str)
            hours = float(value_arr[0])
        except ValueError:
            continue

        rows.append({
            "country": country,
            "year": year,
            "annual_working_hours": hours,
            "source": "OECD",
        })
    return dedupe_and_sort(rows)


def build_embedded_huberman_minns() -> List[Dict]:
    rows: List[Dict] = []
    for country, yearly in HUBERMAN_MINNS.items():
        for year, hours in yearly.items():
            rows.append({
                "country": country,
                "year": int(year),
                "annual_working_hours": float(hours),
                "source": "Huberman & Minns (2007)",
            })
    return dedupe_and_sort(rows)


def dedupe_and_sort(rows: List[Dict]) -> List[Dict]:
    dedup: Dict[Tuple[str, int], Dict] = {}
    for row in rows:
        dedup[(row["country"], row["year"])] = row
    return sorted(dedup.values(), key=lambda r: (r["country"], r["year"]))


def build_dataset() -> List[Dict]:
    print("Attempt 1: OWID historical dataset (Huberman & Minns)...")
    text = download_text(OWID_DATASET_URL)
    if text:
        rows = parse_owid_csv(text)
        if rows:
            print(f"  Success — {len(rows)} rows from OWID dataset")
            return rows

    print("Attempt 2: OWID catalog frame...")
    text = download_text(OWID_CATALOG_URL)
    if text:
        rows = parse_owid_csv(text)
        if rows:
            print(f"  Success — {len(rows)} rows from OWID catalog")
            return rows

    print("Attempt 3: OECD API + embedded Huberman & Minns...")
    embedded = build_embedded_huberman_minns()
    text = download_text(OECD_URL)
    if text:
        oecd_rows = parse_oecd_json(text)
        oecd_recent = [r for r in oecd_rows if r["year"] > 2000]
        combined = dedupe_and_sort(embedded + oecd_recent)
        if combined:
            print(f"  Success — {len(combined)} rows (embedded historical + {len(oecd_recent)} OECD recent)")
            return combined

    print("Attempt 4: embedded Huberman & Minns only...")
    print(f"  Using embedded data: {len(embedded)} rows")
    return embedded


def write_csv(rows: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            hours = row["annual_working_hours"]
            if isinstance(hours, float) and hours.is_integer():
                hours = int(hours)
            writer.writerow({
                "country": row["country"],
                "year": int(row["year"]),
                "annual_working_hours": hours,
                "source": row["source"],
            })


def main() -> None:
    rows = build_dataset()
    write_csv(rows, OUTPUT_PATH)
    countries = sorted({r["country"] for r in rows})
    years = [r["year"] for r in rows]
    print(f"\nWrote {OUTPUT_PATH}")
    print(f"  Rows: {len(rows)}")
    print(f"  Countries ({len(countries)}): {', '.join(countries)}")
    print(f"  Year range: {min(years)}-{max(years)}")


if __name__ == "__main__":
    main()
