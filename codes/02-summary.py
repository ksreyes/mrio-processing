import numpy as np
import pandas as pd
import time
from mrio import MRIO, progress_check, get_years, aggregate_sectors, convert_dtypes

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    df = pd.DataFrame()
    input, output = f'mrio-{version}.parquet', f'summary-{version}.parquet'
    years = get_years(f'data/{input}')

    for year in years:

        mrio = MRIO(f'data/{input}', year)
        G, N = mrio.G, mrio.N

        df_t = pd.DataFrame({
            't': year,
            's': mrio.country_inds().repeat(N),
            'i': np.tile(mrio.sector_inds(), G),
            'i5': np.tile(mrio.sector_inds(agg=5), G),
            'i15': np.tile(mrio.sector_inds(agg=15), G),
            'x': mrio.x.data,
            'zuse': mrio.Z.col_sum().data,
            'va': mrio.va.data,
            'zsales': mrio.Z.row_sum().data,
            'y': mrio.Y.row_sum().data,
            'e': mrio.Z.zeroout().row_sum().data + mrio.Y.zeroout().row_sum().data,
            'ez': mrio.Z.zeroout().row_sum().data,
            'ey': mrio.Y.zeroout().row_sum().data
        })
        df = pd.concat([df, df_t], ignore_index=True)

    summary = aggregate_sectors(
        dataframe = 'df', 
        cols_index = ['t', 's'], 
        cols_to_sum = ['x', 'zuse', 'va', 'zsales', 'y', 'e', 'ez', 'ey']
    )
    summary = convert_dtypes(summary)
    summary.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')