# MRIO Processing Scripts

The ADB Multi-Regional Input-Output Tables are produced by the MRIO team. The scripts in this repo processes the raw Excel files and outputs various datasets used in global value chain analysis. 

The following provides a brief description of each notebook. Note that they must be run in order.

1. `process-mrios.ipynb`
Converts Excel files into machine-readable format and saved as space-optimal parquet files. Results are saved in `data/interim/`.

1. `summary-table.ipynb`
Tabulates key aggregates for each country and country-sector. Results are saved as `summary.csv` in `data/final/`.

1. `trade-accounting.ipynb`
Performs the Borin and Mancini (2019) exports decomposition. Results are saved as `ta.parquet`, `ta-es.parquet`, and `ta-os.parquet` in `data/interim/`.

1. `gvc-participation.ipynb`
Computes the trade-based and production-based GVC participation rates. Results are saved as `gvcp.csv` in `data/final/`.