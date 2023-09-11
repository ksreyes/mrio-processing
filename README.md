# ADB MRIO processing scripts

The scripts in this repo process the ADB MRIO Excel files and implement various indicators from the input–output and global value chain literature. Download the tables [here](https://kidb.adb.org/mrio) and save each version in a seperate folder under `data/raw`. Run the scripts in the order listed below. All outputs are saved in `data` in parquet format.

| Script | Description |
| -------- | ----------- |
| **Preprocessing**<br>[`01-preprocess-mrios.py`](codes/01-preprocess-mrios.py)| Converts raw Excel files into machine-readable format. |
| **Summary tables**<br>[`02-summary-tables.py`](codes/02-summary-tables.py) | Tabulates key aggregates for each country-sector. |
| **Value added flows**<br>[`03-value-added-flows.py`](codes/03-value-added-flows.py) | Computes value added originating from and finally absorbed in each country-sector. |
| **Borin–Mancini decomposition**<br>[`04-exports-decomposition.py`](codes/04-exports-decomposition.py) | Performs the Borin and Mancini (2019) exports decomposition at the sector level. |
| **GVC participation**<br>[`05-gvc-participation.py`](codes/05-gvc-participation.py) | Computes the trade-based and production-based GVC participation rates as defined in ADB (2021). |
| **Production lengths**<br>[`06-production-lengths.py`](codes/06-production-lengths.py) | Computes production lengths following the methodology of Wang, Wei, Yu, and Zhu (2017). |
| **Revealed comparative advantage**<br>[`07-revealed-comparative-advantage.py`](codes/07-revealed-comparative-advantage.py) | Computes revealed comparative advantage indices as defined in ADB (2021). |
| **Regional concentration**<br>[`08-regional-concentration.py`](codes/08-regional-concentration.py) | Computes a measure of regional trade concentration inspired by Frankel (1997). |
| **Deflators**<br>[`09-preprocess-deflators.py`](codes/09-preprocess-deflators.py) | Converts raw Excel files into machine-readable format. |
| **Real effective exchange rate weights**<br>[`10-reer-weights.py`](codes/10-reer-weights.py) | Computes real effective exchange rate weights matrices using the Bems and Johnson (2017) and Patel, Wang, and Wei (2019) methodologies. |
| **Real effective exchange rate indices**<br>[`11-reer.py`](codes/11-reer.py) | Computes the weights matrix and the real effective exchange rate index using the Bems and Johnson (2017) and Patel, Wang, and Wei (2019) methodologies. |
|     |     |

## About the MRIO

The Asian Development Bank [Multiregional Input–Output Tables](https://kidb.adb.org/mrio) are a time series of intercountry input–output tables disaggregated into 35 sectors. Final demand are disaggregated into five categories. The following is a schematic of an MRIO table.

![](images/schematic.jpg)

Three versions of the tables are currently available:

1. **ADB MRIO 72 countries.** Available for 2017–2022.
1. **ADB MRIO 62 countries.** Available for 2000 and 2007–2022.
1. **ADB MRIO 62 countries constant price.** Values pegged to 2010 prices. Available for 2007–2022.

## References

- Asian Development Bank. 2021. ["An analytical framework for studying global value chains."](https://www.adb.org/sites/default/files/publication/720461/ki2021.pdf) In *Key Indicators for Asia and the Pacific 2021*. Mandaluyong City, Philippines: Asian Development Bank.
- Rudolfs Bems and Robert C. Johnson, R. C. (2017). ["Demand for value added and value-added exchange rates."](https://doi.org/10.1257/mac.20150216) *American Economic Journal: Macroeconomics*, 9(4), 45–90.
- Alessandro Borin and Michele Mancini. 2019. [“Measuring what matters in global value chains and value-added trade.”](https://elibrary.worldbank.org/doi/abs/10.1596/1813-9450-8804) *Policy Research Working Paper No. 8804*.
- Jeffrey A. Frankel. 1997. *Regional Trading Blocs in the World Economic System*. Washington, DC: Peterson Institute for International Economics.
- Nikhil Patel, Zhi Wang, and Shang-Jin Wei, S. (2019). ["Global value chains and effective exchange rates at the country- sector level."](https://doi.org/10.1111/jmcb.12670) Journal of Money, Credit, and Banking, 51(1), 7–42.
- Zhi Wang, Shang-Jin Wei, Xinding Yu, and Kunfu Zhu. 2017a. [“Characterizing global value chains: production length and upstreamness.”](https://www.nber.org/papers/w23261) *NBER Working Paper No. 23261*.

## Citation

```bibtex
@misc{ksreyes2023adbmrioscripts,
    title = {{ADB MRIO processing scripts}},
    author = {{Kenneth S. Reyes}},
    url = {https://github.com/ksreyes/adb-mrio},
    year = {2023},
}
```

## Disclaimer

The contents of this repository are not endorsed by the Asian Development Bank or the authors of cited works. All errors are my own.