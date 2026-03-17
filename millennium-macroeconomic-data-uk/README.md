# A Millennium of Macroeconomic Data for the UK

Bank of England dataset covering UK macroeconomic history from the 13th century (with benchmark estimates from 1086, the year of the Domesday Book) through 2016. Version 3.1.

## Background

Compiled by Ryland Thomas and Nicholas Dimsdale at the Bank of England, this dataset brings together over 130 macroeconomic variables for the United Kingdom spanning nine centuries. It covers national accounts, labour, capital and productivity, wages and prices, financial markets, money and credit, fiscal accounts, and trade.

The dataset is notable for its historical depth: GDP estimates reach back to 1086, prices to 1209, and interest rates to 1694 — making it one of the longest continuous macroeconomic records for any country.

## Data

Three CSV files in `data/`, all in long format (one row per period-variable pair):

### `data/annual.csv`

65 variables at annual frequency, 1086–2016.

| Column | Description |
|--------|-------------|
| `year` | Calendar year |
| `variable_id` | Machine-readable variable slug |
| `variable` | Full variable description |
| `section` | Thematic section (e.g. National Accounts, Wages and Prices) |
| `unit` | Unit of measurement |
| `value` | Observed value |

### `data/quarterly.csv`

48 variables at quarterly frequency. `period` format: `YYYY-Q1` through `YYYY-Q4`.

### `data/monthly.csv`

26 variables at monthly frequency. `period` format: `YYYY-MM`.

## Sources

- Thomas, R. and Dimsdale, N. (2017). [A Millennium of Macroeconomic Data for the UK](https://www.bankofengland.co.uk/statistics/research-datasets). Bank of England OBRA dataset, Version 3.1.

## Reproduce

```bash
python3 process.py
```

Downloads the Bank of England xlsx and extracts the three headline sheets (A1 annual, Q1 quarterly, M1 monthly) into long-format CSVs. The original source file is archived at `archive/millennium-of-macroeconomic-data.xlsx`.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
