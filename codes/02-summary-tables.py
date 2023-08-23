### Choose MRIO version #######################################################
version = '72'        # 72 economies (2017-)
# version = '62'        # 62 economies (2000, 2007-)
# version = '62c'       # 62 economies, constant prices (2007-)
###############################################################################

import numpy as np
import pandas as pd
import duckdb
from functions import zeroout
np.seterr(divide='ignore', invalid='ignore')

input, output = f'mrio-{version}.parquet', f'summary-{version}.parquet'

sectors = duckdb.sql("SELECT * FROM read_csv_auto('dicts/sectors.csv')").df()
years = duckdb.sql(f"SELECT DISTINCT t FROM 'data/{input}' ORDER BY t").df()['t']
rows = duckdb.sql(f"SELECT COUNT(*) FROM 'data/{input}'").df()

N = 35                                              # Number of sectors
G = int((rows.iloc[0, 0] / len(years) - 7) / N)     # Number of countries + 1
f = 5                                               # Number of final demand components

df = pd.DataFrame()

for year in years:
    
    mrio = duckdb.sql(f"SELECT * EXCLUDE(t, si) FROM 'data/{input}' WHERE t={year}").df()
    mrio = mrio.values

    x = mrio[-1][:(G*N)]
    Z = mrio[:(G*N)][:, :(G*N)]
    zuse, zsales = np.sum(Z, axis=0), np.sum(Z, axis=1)
    va = np.sum(mrio[-7:-1][:, :(G*N)], axis=0)
    Y_big = mrio[:(G*N)][:, (G*N):-1]
    Y = Y_big @ np.kron(np.eye(G, dtype=bool), np.ones((f, 1), dtype=bool))
    Zd = zeroout(Z @ np.kron(np.eye(G, dtype=bool), np.ones((N, 1), dtype=bool)), inverse=True)
    Yd = zeroout(Y, inverse=True)
    y, ez, ey = np.sum(Y, axis=1), np.sum(Zd, axis=1), np.sum(Yd, axis=1)

    df_t = pd.DataFrame({
        't': year,
        's': np.arange(1, G+1, dtype=np.uint8).repeat(N),
        'i': np.tile(sectors['c_ind'], G),
        'i5': np.tile(sectors['c5_ind'], G),
        'i15': np.tile(sectors['c15_ind'], G),
        'x': x,
        'zuse': zuse,
        'va': va,
        'zsales': zsales,
        'y': y,
        'e': ez + ey,
        'ez': ez,
        'ey': ey
    })
    df = pd.concat([df, df_t], ignore_index=True)

    print(f'{year} done')

sum_terms = '''
    sum(x) AS x, 
    sum(zuse) AS zuse, 
    sum(va) AS va, 
    sum(zsales) AS zsales, 
    sum(y) AS y, 
    sum(e) AS e, 
    sum(ez) AS ez, 
    sum(ey) AS ey
    '''

summary = duckdb.sql(
    f"""
    (SELECT t, s, 0 AS agg, 0 AS i, {sum_terms}
    FROM df GROUP BY t, s ORDER BY t, s)

    UNION ALL

    (SELECT t, s, 5 AS agg, i5 AS i, {sum_terms}
    FROM df GROUP BY t, s, i5 ORDER BY t, s, i5)

    UNION ALL

    (SELECT t, s, 15 AS agg, i15 AS i, {sum_terms}
    FROM df GROUP BY t, s, i15 ORDER BY t, s, i15)

    UNION ALL

    (SELECT t, s, 35 AS agg, i, {sum_terms}
    FROM df GROUP BY t, s, i ORDER BY t, s, i)
    """
).df()

summary['t'] = summary['t'].astype(np.uint16)
summary['agg'] = summary['agg'].astype(np.uint8)
summary['i'] = summary['i'].astype(np.uint8)
summary['x'] = summary['x'].astype(np.float32)
summary['zuse'] = summary['zuse'].astype(np.float32)
summary['va'] = summary['va'].astype(np.float32)
summary['zsales'] = summary['zsales'].astype(np.float32)
summary['y'] = summary['y'].astype(np.float32)
summary['e'] = summary['e'].astype(np.float32)
summary['ez'] = summary['ez'].astype(np.float32)
summary['ey'] = summary['ey'].astype(np.float32)

summary.to_parquet(f'data/{output}', index=False)