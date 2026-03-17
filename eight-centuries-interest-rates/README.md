# Eight Centuries of Global Real Interest Rates

Annual global real interest rates from 1311 to 2018, reconstructed by Paul Schmelzing for the Bank of England (Staff Working Paper No. 845, 2020).

## Data

The dataset covers 78% of advanced economy GDP over time, drawing on archival, printed primary, and secondary sources across successive monetary and fiscal regimes.

### Files

| File | Description | Rows |
|------|-------------|------|
| `data/headline-rates.csv` | Global real rates, 1311–2018 (annual + 7yr avg) | 708 |
| `data/country-rates.csv` | Country-level real rates, 1314–2018 | 4,414 |

### Key fields

**headline-rates.csv:**
- `global_real_7y` — global real rate, 7-year centred average (primary trend series)
- `global_real` — global real rate, annual
- `safe_real_7y` / `safe_real` — safe-asset-provider (e.g. UK gilts, US Treasuries)

**country-rates.csv:** Italy, UK, Netherlands, Germany, France, United States, Spain, Japan

## Source

> Paul Schmelzing, "Eight Centuries of Global Real Interest Rates, R-G, and the 'Suprasecular' Decline, 1311–2018", Bank of England Staff Working Paper No. 845, 2020.

[Paper](https://www.bankofengland.co.uk/working-paper/2020/eight-centuries-of-global-real-interest-rates-r-g-and-the-suprasecular-decline-1311-2018) | [Data (xlsx)](https://github.com/datasets/awesome-data/files/6604635/eight-centuries-of-global-real-interest-rates-r-g-and-the-suprasecular-decline-1311-2018-data.xlsx)

**License:** Open Government Licence v3.0

## Key finding

Since the monetary upheavals of the late Middle Ages, real interest rates have followed a long-run secular decline of 0.6–1.6 basis points per annum. Currently depressed sovereign real rates are convergingback to this historical trend — not evidence of secular stagnation, but a continuation of an 800-year pattern.

This directly challenges Piketty (2014)'s assumption of stable capital returns: the R-G series shows a downward trend over the same timeframe.

## Reproduce

```bash
pip install openpyxl
python process.py
```
