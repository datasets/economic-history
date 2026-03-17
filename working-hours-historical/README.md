# Working Hours — Historical Time Series

Annual hours of work per worker across 12 countries, 1870–2000. Based on Huberman & Minns (2007).

## Background

Huberman and Minns assembled comparable estimates of annual working hours per worker from national sources across twelve now-rich countries. The dataset spans 1870 to 2000 at roughly decadal benchmark years (plus 1913 and 1938), covering Belgium, Canada, Denmark, France, Germany, Ireland, Netherlands, Norway, Sweden, Switzerland, the United Kingdom, and the United States.

The headline story is stark: workers in 1870 typically put in between 2,600 and 3,800 hours per year — the equivalent of 50–73 hours every week with no holidays. By 2000, that figure had roughly halved to 1,340–1,840 hours. The decline was driven by shorter workdays, the spread of the two-day weekend, paid annual leave, and public holidays.

## Data

### `data/working-hours.csv`

One row per country per benchmark year (180 rows total). Columns:

| Column | Description |
|--------|-------------|
| `country` | Country name |
| `year` | Benchmark year (1870, 1880, …, 1990, 2000; also 1913 and 1938) |
| `annual_working_hours` | Average annual hours worked per worker |
| `source` | Data source (`Huberman & Minns (2007)`) |

**Countries covered:** Belgium, Canada, Denmark, France, Germany, Ireland, Netherlands, Norway, Sweden, Switzerland, United Kingdom, United States

**Year range:** 1870–2000 (15 benchmark years per country)

## Sources

- Huberman, M. and Minns, C. (2007). *The times they are not changin': Days and hours of work in Old and New Worlds, 1870–2000*. European Review of Economic History, 11(1), 45–74. <https://doi.org/10.1016/j.eeh.2006.07.002>

## Reproduce

```bash
python3 process.py
```

Generates `data/working-hours.csv`. The script first attempts to fetch updated data from OWID and OECD; if those sources are unavailable, it falls back to the embedded Huberman & Minns table.
