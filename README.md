# MRIO Processing Scripts

The Asian Development Bank (ADB) Multiregional Input–Output (MRIO) Tables are produced and maintained by a dedicated team in ADB. This is the primary dataset used by the ADB global value chains (GVC) team in its research projects. For more details on input–output analysis and the derivation of GVC indicators, check out this repo's [wiki](https://github.com/adb-sna-gvc/mrio-processing/wiki).

The scripts in this repo process the raw Excel files and outputs various datasets. The following is a brief description of each notebook. Note that they must be run in order.

1. `process-mrios.ipynb`
Loads Excel files from `data/raw/{foldername}/` and converts them into machine-readable format. Outputs are saved as Parquet files in `data/interim/`.

1. `summary-table.ipynb`
Tabulates key aggregates for each country and country-sector. Results are saved as `summary.parquet` in `data/final/`.

1. `trade-accounting.ipynb`
Performs the Borin and Mancini (2019) exports decomposition. Results are saved as `ta.parquet`, `ta-es.parquet`, and `ta-os.parquet` in `data/interim/trade-accounting/`.

1. `gvc-participation.ipynb`
Computes the trade-based and production-based GVC participation rates. Results are saved as `gvcp.parquet` in `data/final/`.

1. `production-lengths.ipynb`
Computes production lengths following the methodology of Wang, Wei, Yu, and Zhu (2017). Results are saved as `lengths.parquet` in `data/final/`.

1. `revealed-comparative-advantage.ipynb`
Computes revealed comparative advantage indices. Results are saved as `rca.parquet` in `data/final/`.