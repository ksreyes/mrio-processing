import numpy as np
import pandas as pd
import time
from mrio import MRIO, progress_check, get_years, aggregate_sectors, convert_dtypes

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    df = pd.DataFrame()
    input, output = f'mrio-{version}.parquet', f'flows-{version}.parquet'
    years = get_years(f'data/{input}')
    
    for year in years:
        
        mrio = MRIO(f'data/{input}', year, full=True)
        G, N = mrio.G, mrio.N
        VBY = mrio.v.diag() @ mrio.B @ mrio.Y

        df_t = pd.DataFrame({
            't': year,
            's': np.tile(mrio.country_inds().repeat(N), G),
            'i': np.tile(mrio.sector_inds(), G*G),
            'i5': np.tile(mrio.sector_inds(agg=5), G*G),
            'i15': np.tile(mrio.sector_inds(agg=15), G*G),
            'r': mrio.country_inds().repeat(G*N),
            'z_flows': mrio.Z.row_sum(chunk=N).asvector().data,
            'y_flows': mrio.Y.asvector().data,
            'va_flows': VBY.asvector().data
        })
        df = pd.concat([df, df_t], ignore_index=True)

    flows = aggregate_sectors(
        dataframe = 'df',
        cols_index = ['t', 's', 'r'],
        cols_to_sum = ['z_flows', 'y_flows', 'va_flows']
    )
    flows = convert_dtypes(flows)
    flows.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')