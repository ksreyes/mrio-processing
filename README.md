# ADB MRIO processing scripts

The Asian Development Bank (ADB) Multiregional Input–Output (MRIO) Tables are a freely available time series of intercountry input–output tables. They have distinct information for 72 countries plus a residual "Rest of the world" entity for the years 2017–2022. A longer time series spanning 2000 and 2007–2021 is also available for a smaller set of 62 countries. Each country is disaggregated into 35 sectors.

The scripts in this repo process the Excel files and derives various indicators from the literature. The following is a brief description of each notebook. Note that they must be run in order.

| --- | --- |
| **Preprocessing**<br>[`01-preprocess-mrios.ipynb`](codes/01-preprocess-mrios.ipynb)| Converts the raw Excel files from `data/raw/{foldername}/` into machine-readable format and saves them as Parquet files in `data/mrio`. |
| **Summary table**<br>[`02-summary-table.ipynb`](codes/02-summary-table.ipynb) | Tabulates key aggregates for each country and country-sector. Results are saved as `summary.parquet` in `data/`. |
| **Borin–Mancini decomposition**<br>[`03-trade-accounting.ipynb`](codes/03-trade-accounting.ipynb) | Performs the Borin and Mancini (2019) exports decomposition. Results are saved as `ta.parquet`, `ta-es.parquet`, and `ta-os.parquet` in `data/trade-accounting/`. |
| **GVC participation**<br>[`04-gvc-participation.ipynb`](codes/04-gvc-participation.ipynb) | Computes the trade-based and production-based GVC participation rates as defined in ADB (2021). Results are saved as `gvcp.parquet` in `data/`. |
| **Production lengths**<br>[`05-production-lengths.ipynb`](codes/05-production-lengths.ipynb) | Computes production lengths following the methodology of Wang, Wei, Yu, and Zhu (2017). Results are saved as `lengths.parquet` in `data/` |
| **Revealed comparative advantage**<br>[`06-revealed-comparative-advantage.ipynb`](codes/06-revealed-comparative-advantage.ipynb) | Computes revealed comparative advantage indices as defined in ADB (2021). Results are saved as `rca.parquet` in `data/`. |
| **Regional concentration**<br>[`07-regional-concentration.ipynb`](codes/07-regional-concentration.ipynb) | Computes a measure of regional trade concentration inspired by Frankel (1997). Results are saved as `rci.parquet` in `data/`. |
| **Real effective exchange rate**<br>[`08-reer.ipynb`](codes/08-reer.ipynb) | Computes the weights matrix and the real effective exchange rate index using the Bems and Johnson (2017) and Patel, Wang, and Wei (2019) methodologies. Uses as an additional data source the MRIO price deflators in `data/raw`. Results are saved as `reer-weights.parquet`, `reer-weights-sector.parquet`, and `reer.parquet` in `data/reer/`. |
|     |     |

## References

- Asian Development Bank. 2021. ["An analytical framework for studying global value chains."](https://www.adb.org/sites/default/files/publication/720461/ki2021.pdf) In *Key Indicators for Asia and the Pacific 2021*. Mandaluyong City, Philippines: Asian Development Bank.
- Rudolfs Bems and Robert C. Johnson, R. C. (2017). ["Demand for value added and value-added exchange rates."](https://doi.org/10.1257/mac.20150216) *American Economic Journal: Macroeconomics*, 9(4), 45–90.
- Alessandro Borin and Michele Mancini. 2019. [“Measuring what matters in global value chains and value-added trade.”](https://elibrary.worldbank.org/doi/abs/10.1596/1813-9450-8804) *Policy Research Working Paper No. 8804*.
- Jeffrey A. Frankel. 1997. *Regional Trading Blocs in the World Economic System*. Washington, DC: Peterson Institute for International Economics.
- Nikhil Patel, Zhi Wang, and Shang-Jin Wei, S. (2019). ["Global value chains and effective exchange rates at the country- sector level."](https://doi.org/10.1111/jmcb.12670) Journal of Money, Credit, and Banking, 51(1), 7–42.
- Zhi Wang, Shang-Jin Wei, Xinding Yu, and Kunfu Zhu. 2017a. [“Characterizing global value chains: production length and upstreamness.”](https://www.nber.org/papers/w23261) *NBER Working Paper No. 23261*.

## Disclaimer

The contents of this repository are in no way endorsed by the Asian Development Bank or the authors of cited works. All errors are my own.