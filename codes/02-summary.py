import pandas as pd
import time
from mrio import MRIO
from utils import get_years, aggregate_sectors, convert_dtypes, ind_pattern, progress_check

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    input = f'mrio-{version}.parquet'
    output = f'summary-{version}.parquet'
    years = get_years(f'data/{input}')
    df = pd.DataFrame()

    for year in years:

        mrio = MRIO(f'data/{input}', year)

        df_t = pd.DataFrame({
            't': year,
            's': ind_pattern(mrio.country_inds(), repeat=mrio.N),
            'i': ind_pattern(mrio.sector_inds(), tile=mrio.G),
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
        table = 'df', 
        cols_index = ['t', 's'], 
        cols_to_sum = ['x', 'zuse', 'va', 'zsales', 'y', 'e', 'ez', 'ey']
    )
    summary = convert_dtypes(summary)
    summary.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')