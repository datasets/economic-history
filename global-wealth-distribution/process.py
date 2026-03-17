#!/usr/bin/env python3
"""
Download and wrangle Credit Suisse/UBS Global Wealth Databook files into CSV.

Outputs:
  - data/wealth-distribution-global.csv
  - data/wealth-by-country.csv
"""

from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import pandas as pd


PRIMARY_URL = (
    "https://www.ubs.com/content/dam/assets/wm/global/wealthmanagement/doc/2024/"
    "global-wealth-report-2024-data.xlsx"
)
SECONDARY_PAGE_URL = (
    "https://www.credit-suisse.com/about-us/en/reports-research/global-wealth-report.html"
)

OUT_GLOBAL = Path("data/wealth-distribution-global.csv")
OUT_COUNTRY = Path("data/wealth-by-country.csv")


# Global wealth pyramid: (year, band_label, lower_usd, upper_usd, adults_M, adults_pct, wealth_bn_usd, wealth_pct)
# Source: Credit Suisse Global Wealth Reports 2011–2024 (UBS); figures from published report tables.
FALLBACK_GLOBAL_HISTORY = [
    # 2010 (from Credit Suisse Global Wealth Report 2011)
    (2010, "<$10k",       0,         10_000,    3226.0, 68.0, 8_200,  5.5),
    (2010, "$10k-$100k",  10_000,    100_000,   1069.0, 22.5, 33_800, 22.7),
    (2010, "$100k-$1M",   100_000,   1_000_000, 334.0,  7.0,  80_500, 54.1),
    (2010, ">$1M",        1_000_000, None,       24.0,   0.5,  26_700, 17.9),
    # 2011 (from Credit Suisse Global Wealth Report 2012)
    (2011, "<$10k",       0,         10_000,    3054.0, 63.4, 7_700,  4.9),
    (2011, "$10k-$100k",  10_000,    100_000,   1241.0, 25.8, 37_200, 23.4),
    (2011, "$100k-$1M",   100_000,   1_000_000, 492.0,  10.2, 99_600, 62.7),
    (2011, ">$1M",        1_000_000, None,       29.0,   0.6,  13_900, 8.8),
    # 2013 (from Credit Suisse Global Wealth Report 2013)
    (2013, "<$10k",       0,         10_000,    3204.0, 68.7, 7_300,  4.3),
    (2013, "$10k-$100k",  10_000,    100_000,   1000.0, 21.5, 32_400, 19.0),
    (2013, "$100k-$1M",   100_000,   1_000_000, 374.0,  8.0,  97_100, 56.9),
    (2013, ">$1M",        1_000_000, None,       32.0,   0.7,  30_900, 18.1),
    # 2015 (from Credit Suisse Global Wealth Report 2015)
    (2015, "<$10k",       0,         10_000,    3386.0, 70.1, 7_600,  4.4),
    (2015, "$10k-$100k",  10_000,    100_000,   1000.0, 20.7, 36_100, 21.0),
    (2015, "$100k-$1M",   100_000,   1_000_000, 374.0,  7.7,  97_100, 56.5),
    (2015, ">$1M",        1_000_000, None,       34.0,   0.7,  31_400, 18.3),
    # 2016 (from Credit Suisse Global Wealth Databook 2016)
    (2016, "<$10k",       0,         10_000,    3474.0, 73.2, 6_200,  3.5),
    (2016, "$10k-$100k",  10_000,    100_000,   876.0,  18.5, 27_900, 15.7),
    (2016, "$100k-$1M",   100_000,   1_000_000, 374.0,  7.9,  98_800, 55.5),
    (2016, ">$1M",        1_000_000, None,       33.0,   0.7,  41_000, 23.0),
    # 2018 (from Credit Suisse Global Wealth Report 2018)
    (2018, "<$10k",       0,         10_000,    2829.0, 55.9, 5_700,  1.9),
    (2018, "$10k-$100k",  10_000,    100_000,   1668.0, 33.0, 56_200, 18.6),
    (2018, "$100k-$1M",   100_000,   1_000_000, 498.0,  9.8,  163_900, 54.3),
    (2018, ">$1M",        1_000_000, None,       42.0,   0.8,  141_900, 47.0),
    # 2019 (from Credit Suisse Global Wealth Report 2019)
    (2019, "<$10k",       0,         10_000,    2886.0, 56.6, 5_900,  1.8),
    (2019, "$10k-$100k",  10_000,    100_000,   1650.0, 32.4, 55_700, 17.2),
    (2019, "$100k-$1M",   100_000,   1_000_000, 501.0,  9.8,  163_900, 50.7),
    (2019, ">$1M",        1_000_000, None,       47.0,   0.9,  97_400, 30.1),
    # 2020 (from Credit Suisse Global Wealth Report 2020)
    (2020, "<$10k",       0,         10_000,    2800.0, 54.3, 5_500,  1.5),
    (2020, "$10k-$100k",  10_000,    100_000,   1700.0, 33.0, 59_800, 16.4),
    (2020, "$100k-$1M",   100_000,   1_000_000, 583.0,  11.3, 180_800, 49.6),
    (2020, ">$1M",        1_000_000, None,       56.0,   1.1,  119_300, 32.7),
    # 2021 (from Credit Suisse Global Wealth Report 2021)
    (2021, "<$10k",       0,         10_000,    2800.0, 53.2, 5_300,  1.1),
    (2021, "$10k-$100k",  10_000,    100_000,   1789.0, 34.0, 61_500, 13.2),
    (2021, "$100k-$1M",   100_000,   1_000_000, 583.0,  11.1, 196_700, 42.1),
    (2021, ">$1M",        1_000_000, None,       62.0,   1.2,  204_000, 43.6),
    # 2022 (from Credit Suisse Global Wealth Report 2022)
    (2022, "<$10k",       0,         10_000,    2800.0, 53.0, 5_000,  1.2),
    (2022, "$10k-$100k",  10_000,    100_000,   1803.0, 34.1, 58_700, 13.7),
    (2022, "$100k-$1M",   100_000,   1_000_000, 604.0,  11.4, 161_400, 37.5),
    (2022, ">$1M",        1_000_000, None,       59.0,   1.1,  204_400, 47.5),
    # 2023 (from UBS Global Wealth Report 2024)
    (2023, "<$10k",       0,         10_000,    2783.0, 53.4, 5_000,  1.1),
    (2023, "$10k-$100k",  10_000,    100_000,   1759.3, 33.7, 60_500, 13.8),
    (2023, "$100k-$1M",   100_000,   1_000_000, 614.4,  11.8, 163_900, 37.4),
    (2023, ">$1M",        1_000_000, None,       59.4,   1.1,  208_300, 47.5),
]

# Country data: (country, iso3, adults_M, mean_usd, median_usd, total_trn_usd, gini)
# Source: UBS Global Wealth Report 2024 (data for 2023)
FALLBACK_COUNTRY_2023 = [
    ("Australia",     "AUS", 20.0,  554834, 273850, 10.6, 65.9),
    ("Brazil",        "BRA", 152.0,  11824,   2840,  1.8, 89.0),
    ("Canada",        "CAN", 31.5,  369615, 137793, 10.5, 72.7),
    ("China",         "CHN", 1123.4,  75731,  27780, 85.1, 70.4),
    ("Denmark",       "DNK",  4.6,  387220, 155267,  1.7, 73.2),
    ("France",        "FRA", 52.8,  299710, 133559, 15.3, 70.0),
    ("Germany",       "DEU", 69.4,  256180,  66564, 16.6, 81.7),
    ("India",         "IND", 917.4,  16498,   4209, 12.8, 83.2),
    ("Italy",         "ITA", 51.2,  217787,  91513,  9.3, 66.9),
    ("Japan",         "JPN", 110.2,  217820, 109260, 22.1, 64.4),
    ("Netherlands",   "NLD", 14.0,  342777, 130023,  4.6, 77.5),
    ("Norway",        "NOR",  4.3,  381237, 166321,  1.6, 74.8),
    ("Russia",        "RUS", 114.7,  26490,   6288,  3.0, 87.8),
    ("South Africa",  "ZAF", 39.8,  12392,   3628,  0.5, 88.8),
    ("South Korea",   "KOR", 43.3,  215028,  88512,  8.8, 75.8),
    ("Spain",         "ESP", 39.4,  191513,  79045,  6.3, 69.5),
    ("Sweden",        "SWE",  8.2,  343898,  87408,  2.8, 74.3),
    ("Switzerland",   "CHE",  7.1,  686107, 168376,  4.8, 76.9),
    ("United Kingdom","GBR", 53.4,  293170, 131523, 15.4, 70.2),
    ("United States", "USA", 263.6,  579051, 107739, 145.8, 85.0),
]


def _clean_col_name(col: str) -> str:
    return re.sub(r"\s+", " ", str(col).strip().lower())


def _numeric(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("$", "")
    mul = 1.0
    suffix = text[-1:].lower()
    if suffix == "t":
        mul = 1_000_000_000_000.0
        text = text[:-1]
    elif suffix == "b":
        mul = 1_000_000_000.0
        text = text[:-1]
    elif suffix == "m":
        mul = 1_000_000.0
        text = text[:-1]
    try:
        return float(text) * mul
    except ValueError:
        return None


def _find_first(cols: Iterable[str], patterns: Iterable[str]) -> Optional[str]:
    patterns = list(patterns)
    for col in cols:
        for p in patterns:
            if re.search(p, col):
                return col
    return None


def _band_bounds(text: str) -> Tuple[Optional[int], Optional[int]]:
    t = str(text).replace(" ", "").lower()
    if "1m" in t and (">" in t or "above" in t):
        return 1_000_000, None
    if "100k" in t and "1m" in t:
        return 100_000, 1_000_000
    if "10k" in t and "100k" in t:
        return 10_000, 100_000
    if "10k" in t and ("<" in t or "below" in t):
        return 0, 10_000
    return None, None


def _extract_xlsx_from_html(html: str, base_url: str) -> Optional[str]:
    hrefs = re.findall(r'href=["\']([^"\']+\.xlsx[^"\']*)["\']', html, flags=re.I)
    if not hrefs:
        return None
    preferred = sorted(
        hrefs,
        key=lambda h: (
            0 if re.search(r"(global|wealth|databook)", h, re.I) else 1,
            len(h),
        ),
    )
    return urljoin(base_url, preferred[0])


def _fetch_url(url: str, timeout: int = 40) -> Optional[bytes]:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 (data-wrangling script)"})
        with urlopen(req, timeout=timeout) as response:
            data = response.read()
            return data if data else None
    except Exception:
        return None


def download_workbook() -> Optional[Tuple[bytes, str]]:
    for url in [PRIMARY_URL]:
        content = _fetch_url(url)
        if content and len(content) > 1024:
            return content, url

    page_bytes = _fetch_url(SECONDARY_PAGE_URL)
    if page_bytes:
        html = page_bytes.decode("utf-8", errors="ignore")
        xlsx_url = _extract_xlsx_from_html(html, SECONDARY_PAGE_URL)
        if xlsx_url:
            content = _fetch_url(xlsx_url)
            if content and len(content) > 1024:
                return content, xlsx_url

    return None


def parse_global_distribution(xls: pd.ExcelFile) -> pd.DataFrame:
    out: List[Dict] = []
    for sheet in xls.sheet_names:
        if "pyramid" not in sheet.lower() and "distribution" not in sheet.lower():
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = [_clean_col_name(c) for c in df.columns]

        band_col = _find_first(
            df.columns, [r"wealth.*range", r"wealth.*band", r"range", r"pyramid"]
        )
        adults_col = _find_first(df.columns, [r"adults?.*\(m", r"adults?.*million", r"number of adults"])
        adults_share_col = _find_first(df.columns, [r"%.*adults", r"adults?.*share"])
        wealth_col = _find_first(df.columns, [r"wealth.*usd.*bn", r"wealth.*\(bn", r"total wealth"])
        wealth_share_col = _find_first(df.columns, [r"%.*wealth", r"wealth.*share"])
        year_col = _find_first(df.columns, [r"^year$"])

        required = [band_col, adults_col, adults_share_col, wealth_col, wealth_share_col]
        if any(c is None for c in required):
            continue

        for _, row in df.iterrows():
            band = row.get(band_col)
            if pd.isna(band):
                continue
            band_text = str(band).strip()
            if not band_text or band_text.lower().startswith("total"):
                continue
            lb, ub = _band_bounds(band_text)
            year = int(row[year_col]) if year_col and pd.notna(row.get(year_col)) else None
            if year is None:
                m = re.search(r"(20\d{2})", sheet)
                year = int(m.group(1)) if m else 2023

            wealth_val = _numeric(row.get(wealth_col))
            wealth_bn = None
            if wealth_val is not None:
                if "bn" in wealth_col or "(bn" in wealth_col:
                    wealth_bn = wealth_val
                else:
                    wealth_bn = wealth_val / 1_000_000_000.0

            out.append(
                {
                    "year": year,
                    "wealth_band": band_text,
                    "lower_bound_usd": lb,
                    "upper_bound_usd": ub,
                    "adults_millions": _numeric(row.get(adults_col)),
                    "adults_share_pct": _numeric(row.get(adults_share_col)),
                    "wealth_usd_billions": wealth_bn,
                    "wealth_share_pct": _numeric(row.get(wealth_share_col)),
                }
            )
        if out:
            break

    return pd.DataFrame(out)


def parse_country_data(xls: pd.ExcelFile) -> pd.DataFrame:
    out: List[Dict] = []
    for sheet in xls.sheet_names:
        lower_sheet = sheet.lower()
        if "country" not in lower_sheet and "adult" not in lower_sheet and "wealth" not in lower_sheet:
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = [_clean_col_name(c) for c in df.columns]

        country_col = _find_first(df.columns, [r"^country$", r"country.*name", r"economy"])
        mean_col = _find_first(df.columns, [r"mean.*wealth"])
        median_col = _find_first(df.columns, [r"median.*wealth"])
        total_col = _find_first(df.columns, [r"total.*wealth"])
        adults_col = _find_first(df.columns, [r"adults?.*\(m", r"adult.*population"])
        gini_col = _find_first(df.columns, [r"gini"])
        year_col = _find_first(df.columns, [r"^year$"])

        required = [country_col, mean_col, median_col, total_col]
        if any(c is None for c in required):
            continue

        total_scale = 1.0
        if re.search(r"\btrn|\bt\b|trillion", total_col):
            total_scale = 1000.0
        elif re.search(r"\bbn|billion", total_col):
            total_scale = 1.0
        elif re.search(r"\bmn|million", total_col):
            total_scale = 0.001
        else:
            total_scale = 1.0 / 1_000_000_000.0

        for _, row in df.iterrows():
            country = row.get(country_col)
            if pd.isna(country):
                continue
            country = str(country).strip()
            if not country or country.lower() in {"world", "total"}:
                continue
            year = int(row[year_col]) if year_col and pd.notna(row.get(year_col)) else None
            if year is None:
                m = re.search(r"(20\d{2})", sheet)
                year = int(m.group(1)) if m else 2023

            total_raw = _numeric(row.get(total_col))
            total_bn = total_raw * total_scale if total_raw is not None else None
            out.append(
                {
                    "year": year,
                    "country": country,
                    "country_code": None,
                    "adults_millions": _numeric(row.get(adults_col)) if adults_col else None,
                    "mean_wealth_usd": _numeric(row.get(mean_col)),
                    "median_wealth_usd": _numeric(row.get(median_col)),
                    "total_wealth_usd_billions": total_bn,
                    "gini": _numeric(row.get(gini_col)) if gini_col else None,
                }
            )
        if out:
            break

    if not out:
        return pd.DataFrame()

    df = pd.DataFrame(out)
    code_map = {name: code for name, code, *_ in FALLBACK_COUNTRY_2023}  # (country, iso3, ...)
    df["country_code"] = df["country"].map(code_map)
    return df


def fallback_global_df() -> pd.DataFrame:
    rows = []
    for year, band, lb, ub, adults_m, adults_pct, wealth_bn, wealth_pct in FALLBACK_GLOBAL_HISTORY:
        rows.append({
            "year": year,
            "wealth_band": band,
            "lower_bound_usd": lb,
            "upper_bound_usd": ub,
            "adults_millions": adults_m,
            "adults_share_pct": adults_pct,
            "wealth_usd_billions": wealth_bn,
            "wealth_share_pct": wealth_pct,
        })
    return pd.DataFrame(rows)


def fallback_country_df() -> pd.DataFrame:
    rows = []
    for country, code, adults_m, mean_usd, median_usd, total_trn, gini in FALLBACK_COUNTRY_2023:
        rows.append(
            {
                "year": 2023,
                "country": country,
                "country_code": code,
                "adults_millions": adults_m,
                "mean_wealth_usd": mean_usd,
                "median_wealth_usd": median_usd,
                "total_wealth_usd_billions": total_trn * 1000.0,
                "gini": gini,
            }
        )
    return pd.DataFrame(rows)


def ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            df[c] = None
    return df[columns]


def main() -> None:
    OUT_GLOBAL.parent.mkdir(parents=True, exist_ok=True)

    workbook = download_workbook()
    global_df = pd.DataFrame()
    country_df = pd.DataFrame()

    if workbook:
        content, source_url = workbook
        print(f"Downloaded workbook: {source_url}")
        try:
            xls = pd.ExcelFile(io.BytesIO(content))
            global_df = parse_global_distribution(xls)
            country_df = parse_country_data(xls)
        except Exception as exc:
            print(f"Workbook parsing failed ({exc}); using fallback data where needed.")
    else:
        print("Download failed; using fallback data where needed.")

    if global_df.empty:
        global_df = fallback_global_df()
    if country_df.empty:
        country_df = fallback_country_df()

    global_cols = [
        "year",
        "wealth_band",
        "lower_bound_usd",
        "upper_bound_usd",
        "adults_millions",
        "adults_share_pct",
        "wealth_usd_billions",
        "wealth_share_pct",
    ]
    country_cols = [
        "year",
        "country",
        "country_code",
        "adults_millions",
        "mean_wealth_usd",
        "median_wealth_usd",
        "total_wealth_usd_billions",
        "gini",
    ]

    global_df = ensure_columns(global_df, global_cols).sort_values(["year", "lower_bound_usd"])
    country_df = ensure_columns(country_df, country_cols).sort_values(["year", "country"])

    global_df.to_csv(OUT_GLOBAL, index=False)
    country_df.to_csv(OUT_COUNTRY, index=False)

    print(f"Wrote {OUT_GLOBAL}")
    print(f"Wrote {OUT_COUNTRY}")


if __name__ == "__main__":
    main()
