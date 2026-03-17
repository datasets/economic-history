# Global Wealth Distribution

Distribution of wealth globally, drawn from the Credit Suisse Global Wealth Databook (2010–2022) and the UBS Global Wealth Report 2024 (2023 data).

## Background

Every year since 2010, Credit Suisse (now UBS) has published a Global Wealth Report estimating how wealth is distributed across the world's adult population. The dataset covers the global wealth pyramid — how adults are distributed across four wealth bands — and country-level statistics for 20 major economies.

The headline finding: the top 1% of adults (those with more than USD 1M in wealth) hold roughly 45% of all global private wealth, while the bottom 55% (under USD 10k) hold less than 2%.

## Data

### `data/wealth-distribution-global.csv`

Global wealth pyramid by wealth band, 2010–2023. One row per band per year.

| Column | Description |
|--------|-------------|
| `year` | Year of observation |
| `wealth_band` | Wealth band label (under USD 10k, USD 10k–100k, USD 100k–1M, over USD 1M) |
| `lower_bound_usd` | Lower bound of the band in USD |
| `upper_bound_usd` | Upper bound of the band in USD (empty for top band) |
| `adults_millions` | Adults in this band (millions) |
| `adults_share_pct` | Share of global adult population (%) |
| `wealth_usd_billions` | Total wealth held by this band (USD billions) |
| `wealth_share_pct` | Share of global private wealth held by this band (%) |

### `data/wealth-by-country.csv`

Country-level wealth statistics for 20 major economies, 2023.

| Column | Description |
|--------|-------------|
| `year` | Year of observation |
| `country` | Country name |
| `country_code` | ISO 3166-1 alpha-3 code |
| `adults_millions` | Adult population (millions) |
| `mean_wealth_usd` | Mean wealth per adult, in USD |
| `median_wealth_usd` | Median wealth per adult, in USD |
| `total_wealth_usd_billions` | Total household wealth, in USD billions |
| `gini` | Wealth Gini coefficient (0–100; higher = more unequal) |

## Sources

- Credit Suisse Research Institute. [Global Wealth Databook 2016](http://publications.credit-suisse.com/tasks/render/file/index.cfm?fileid=AD6F2B43-B17B-345E-E20A1A254A3E24A5).
- UBS. [Global Wealth Report 2024](https://www.ubs.com/global/en/wealth-management/insights/chief-investment-office/life-goals/2024/global-wealth-report.html).

## Reproduce

```bash
python3 process.py
```

## License

[Open Data Commons Public Domain Dedication and License (PDDL)](https://opendatacommons.org/licenses/pddl/1-0/)
