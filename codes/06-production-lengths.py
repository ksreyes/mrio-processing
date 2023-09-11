import numpy as np
import pandas as pd
import duckdb
from functions import zeroout, get_fatdiag
np.seterr(divide='ignore', invalid='ignore')

mrio_versions = ['72', '62', '62c']
sectors = duckdb.sql("SELECT * FROM read_csv_auto('dicts/sectors.csv')").df()
N, f = 35, 5

start = time.time()

for version in mrio_versions:

    input, output = f'mrio-{version}.parquet', f'apl-{version}.parquet'

    years = duckdb.sql(f"SELECT DISTINCT t FROM 'data/{input}' ORDER BY t").df()['t']
    rows = duckdb.sql(f"SELECT COUNT(*) FROM 'data/{input}'").df()
    G = int((rows.iloc[0, 0] / len(years) - 7) / N)

    df = pd.DataFrame()

    for year in years:
        
        mrio = duckdb.sql(f"SELECT * EXCLUDE(t, si) FROM 'data/{input}' WHERE t={year}").df()
        mrio = mrio.values.astype(np.float32)

        x = mrio[-1][:(G*N)]
        Z = mrio[:(G*N)][:, :(G*N)]
        va = np.sum(mrio[-7:-1][:, :(G*N)], axis=0)
        Y_big = mrio[:(G*N)][:, (G*N):-1]
        Y = Y_big @ np.kron(np.eye(G, dtype=np.uint8), np.ones((f, 1), dtype=np.uint8))
        y = np.sum(Y, axis=1)
        yd = get_fatdiag(Y)
        yf = y - yd
        v = np.where(x != 0, va / x, 0)
        Dx = np.diag(np.where(x != 0, 1 / x, 0))
        A = Z @ Dx
        Ad, Af = zeroout(A, inverse=True), zeroout(A)
        B = np.linalg.inv(np.eye(G*N, dtype=np.uint8) - A)
        Bd = np.linalg.inv(np.eye(G*N, dtype=np.uint8) - Ad)

        X = np.diag(v) @ B @ B @ np.diag(y)
        X_D = np.diag(v) @ Bd @ Bd @ np.diag(yd)
        X_RT = np.diag(v) @ Bd @ Bd @ np.diag(yf)
        Xd_GVC = np.diag(v) @ Bd @ Bd @ Af @ B @ np.diag(y)
        E_GVC = np.diag(v) @ B @ Af @ B @ np.diag(y)
        Xf_GVC = np.diag(v) @ Bd @ Af @ B @ Ad @ B @ np.diag(y)
        VY_D = np.diag(v) @ Bd @ np.diag(yd)
        VY_RT = np.diag(v) @ Bd @ np.diag(yf)
        VY_GVC = np.diag(v) @ Bd @ Af @ B @ np.diag(y)

        df_t = pd.DataFrame({
            't': year, 
            's': np.arange(1, G+1, dtype=np.uint8).repeat(N),
            'i': np.tile(sectors['c_ind'], G), 
            'i5': np.tile(sectors['c5_ind'], G), 
            'i15': np.tile(sectors['c15_ind'], G),
            'va': va, 
            'y': y,
            'Xv': np.sum(X, axis=1),
            'Xv_D': np.sum(X_D, axis=1),
            'Xv_RT': np.sum(X_RT, axis=1),
            'Xvd_GVC': np.sum(Xd_GVC, axis=1),
            'Ev_GVC': np.sum(E_GVC, axis=1),
            'Xvf_GVC': np.sum(Xf_GVC, axis=1),
            'V_D': np.sum(VY_D, axis=1),
            'V_RT': np.sum(VY_RT, axis=1),
            'V_GVC': np.sum(VY_GVC, axis=1),
            'Xy': np.sum(X, axis=0),
            'Xy_D': np.sum(X_D, axis=0),
            'Xy_RT': np.sum(X_RT, axis=0),
            'Xyd_GVC': np.sum(Xd_GVC, axis=0),
            'Ey_GVC': np.sum(E_GVC, axis=0),
            'Xyf_GVC': np.sum(Xf_GVC, axis=0),
            'Y_D': np.sum(VY_D, axis=0),
            'Y_RT': np.sum(VY_RT, axis=0),
            'Y_GVC': np.sum(VY_GVC, axis=0)
        })
        df = pd.concat([df, df_t], ignore_index=True)
        
        print(f'{year} done')

    sum_terms = '''
        sum(Xv) / sum(va) AS PLv, 
        sum(Xv_D) / sum(V_D) AS PLv_D, 
        sum(Xv_RT) / sum(V_RT) AS PLv_RT, 
        sum(Xvd_GVC) / sum(V_GVC) AS PLvd_GVC, 
        sum(Ev_GVC) / sum(V_GVC) AS CBv_GVC, 
        sum(Xvf_GVC) / sum(V_GVC) AS PLvf_GVC, 
        sum(Xy) / sum(y) AS PLy, 
        sum(Xy_D) / sum(Y_D) AS PLy_D, 
        sum(Xy_RT) / sum(Y_RT) AS PLy_RT, 
        sum(Xyd_GVC) / sum(Y_GVC) AS PLyd_GVC, 
        sum(Ey_GVC) / sum(Y_GVC) AS CBy_GVC, 
        sum(Xyf_GVC) / sum(Y_GVC) AS PLyf_GVC
        '''

    apl = duckdb.sql(
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

    apl['t'] = apl['t'].astype(np.uint16)
    apl['agg'] = apl['agg'].astype(np.uint8)
    apl['i'] = apl['i'].astype(np.uint8)

    apl['PLv'] = apl['PLv'].astype(np.float32)
    apl['PLv_D'] = apl['PLv_D'].astype(np.float32)
    apl['PLv_RT'] = apl['PLv_RT'].astype(np.float32)
    apl['PLvd_GVC'] = apl['PLvd_GVC'].astype(np.float32)
    apl['CBv_GVC'] = apl['CBv_GVC'].astype(np.float32)
    apl['PLvf_GVC'] = apl['PLvf_GVC'].astype(np.float32)

    apl['PLy'] = apl['PLy'].astype(np.float32)
    apl['PLy_D'] = apl['PLy_D'].astype(np.float32)
    apl['PLy_RT'] = apl['PLy_RT'].astype(np.float32)
    apl['PLyd_GVC'] = apl['PLyd_GVC'].astype(np.float32)
    apl['CBy_GVC'] = apl['CBy_GVC'].astype(np.float32)
    apl['PLyf_GVC'] = apl['PLyf_GVC'].astype(np.float32)

    apl['PLv_GVC'] = apl['PLvd_GVC'] + apl['CBv_GVC'] + apl['PLvf_GVC']
    apl['PLy_GVC'] = apl['PLyd_GVC'] + apl['CBy_GVC'] + apl['PLyf_GVC']
    apl['GVC_POS'] = apl['PLv_GVC'] / apl['PLy_GVC']

    apl.to_parquet(f'data/{output}', index=False)