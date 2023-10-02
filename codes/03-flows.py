import pandas as pd
import time
from mrio import MRIO
from utils import get_years, ind_pattern, aggregate_sectors, convert_dtypes, progress_check

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    input = f'mrio-{version}.parquet'
    output = f'flows-{version}.parquet'
    years = get_years(f'data/{input}')
    df = pd.DataFrame()
    
    for year in years:
        
        mrio = MRIO(f'data/{input}', year, full=True)
        G, N = mrio.G, mrio.N
        VBY = mrio.v.diag() @ mrio.B @ mrio.Y

        df_t = pd.DataFrame({
            't': year,
            's': ind_pattern(mrio.country_inds(), repeat=N, tile=G),
            'i': ind_pattern(mrio.sector_inds(), tile=G*G),
            'r': ind_pattern(mrio.country_inds(), repeat=G*N),
            'z_flows': mrio.Z.row_sum(chunk=N).asvector().data,
            'y_flows': mrio.Y.asvector().data,
            'va_flows': VBY.asvector().data
        })
        df = pd.concat([df, df_t], ignore_index=True)

    flows = aggregate_sectors(
        table = 'df',
        cols_index = ['t', 's', 'r'],
        cols_to_sum = ['z_flows', 'y_flows', 'va_flows']
    )
    flows = convert_dtypes(flows)
    flows.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')