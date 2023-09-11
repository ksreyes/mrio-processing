import numpy as np
import pandas as pd
import duckdb
import time
from functions import asvector
np.seterr(divide='ignore', invalid='ignore')

mrio_versions = ['72', '62', '62c']
sectors = duckdb.sql("SELECT * FROM read_csv_auto('dicts/sectors.csv')").df()
N, f = 35, 5

start = time.time()

for version in mrio_versions:

    input, output = f'mrio-{version}.parquet', f'flows-{version}.parquet'

    years = duckdb.sql(f"SELECT DISTINCT t FROM 'data/{input}' ORDER BY t").df()['t']
    rows = duckdb.sql(f"SELECT count(*) FROM 'data/{input}'").df()
    G = int((rows.iloc[0, 0] / len(years) - 7) / N)

    df = pd.DataFrame()

    for year in years:
        
        mrio = duckdb.sql(f"SELECT * EXCLUDE(t, si) FROM 'data/{input}' WHERE t={year}").df()
        mrio = mrio.values.astype(np.float32)

        x = mrio[-1][:(G*N)]
        Z = mrio[:(G*N)][:, :(G*N)]
        va = np.sum(mrio[-7:-1][:, :(G*N)], axis=0)
        v = np.where(x != 0, va/x, 0)
        A = Z @ np.diag(np.where(x != 0, 1/x, 0))
        B = np.linalg.inv(np.eye(G*N, dtype=bool) - A)
        Y_big = mrio[:(G*N)][:, (G*N):-1]
        Y = Y_big @ np.kron(np.eye(G, dtype=bool), np.ones((f, 1), dtype=bool))
        VBY = np.diag(v) @ B @ Y

        df_t = pd.DataFrame({
            't': year,
            's': np.tile(np.arange(1, G+1, dtype=np.uint8).repeat(N), G),
            'r': np.arange(1, G+1, dtype=np.uint8).repeat(G*N),
            'i': np.tile(sectors['c_ind'], G*G),
            'i5': np.tile(sectors['c5_ind'], G*G),
            'i15': np.tile(sectors['c15_ind'], G*G),
            'flows': asvector(VBY)
        })
        
        df = pd.concat([df, df_t], ignore_index=True)

    flows = duckdb.sql(
        f"""
        (SELECT t, s, r, 0 AS agg, 0 AS i, sum(flows) AS flows
        FROM df GROUP BY t, s, r ORDER BY t, s, r)

        UNION ALL

        (SELECT t, s, r, 5 AS agg, i5 AS i, sum(flows) AS flows
        FROM df GROUP BY t, s, r, i5 ORDER BY t, s, r, i5)

        UNION ALL

        (SELECT t, s, r, 15 AS agg, i15 AS i, sum(flows) AS flows
        FROM df GROUP BY t, s, r, i15 ORDER BY t, s, r, i15)

        UNION ALL

        (SELECT t, s, r, 35 AS agg, i, sum(flows) AS flows
        FROM df GROUP BY t, s, r, i ORDER BY t, s, r, i)
        """
    ).df()

    flows['t'] = flows['t'].astype(np.uint16)
    flows['agg'] = flows['agg'].astype(np.uint8)
    flows['i'] = flows['i'].astype(np.uint8)
    flows['flows'] = flows['flows'].astype(np.float32)

    flows.to_parquet(f'data/{output}', index=False)

    # Time check
    checkpoint = time.time()
    elapsed = checkpoint - start
    time_elapsed = f'{int(elapsed // 60)} mins {round(elapsed % 60, 1)} secs'

    print(f'\nMRIO-{version} done. \nTime elapsed: {time_elapsed}.')
