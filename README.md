# MRIO Processing Scripts

The ADB Multi-Regional Input-Output Tables are produced by the MRIO team. The scripts in this repo processes the raw Excel files and outputs various datasets used in global value chain analysis. 

The following provides a brief description of each notebook.

1. `process-mrios.ipynb`
Converts Excel files into machine-readable format and saved as space-optimal parquet files. Outputs are at `data/interim/`.

1. `trade-accounting.ipynb`
Performs the Borin and Mancini (2019) decomposition. 