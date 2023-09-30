import numpy as np
import pandas as pd
import duckdb
import time
from mrio import MRIO, progress_check, get_years, convert_dtypes

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    df = pd.DataFrame()
    input, output = f'mrio-{version}.parquet', f'apl-{version}.parquet'
    years = get_years(f'data/{input}')

    for year in years:

        mrio = MRIO(f'data/{input}', year, full=True)
        G, N = mrio.G, mrio.N
        
        v = mrio.v.diag()
        y = mrio.Y.row_sum().diag()
        yd = mrio.Y.get_fatdiag().diag()
        yf = (mrio.Y.row_sum() - mrio.Y.get_fatdiag()).diag()
        Ad = mrio.A.zeroout(inverse=True)
        Af = mrio.A.zeroout()
        B = mrio.B
        Bd = (mrio.I(G*N) - Ad).invert()

        X = v @ B @ B @ y
        X_D = v @ Bd @ Bd @ yd
        X_RT = v @ Bd @ Bd @ yf
        Xd_GVC = v @ Bd @ Bd @ Af @ B @ y
        E_GVC = v @ B @ Af @ B @ y
        Xf_GVC = v @ Bd @ Af @ B @ Ad @ B @ y
        VY_D = v @ Bd @ yd
        VY_RT = v @ Bd @ yf
        VY_GVC = v @ Bd @ Af @ B @ y

        df_t = pd.DataFrame({
            't': year, 
            's': mrio.country_inds().repeat(N),
            'i': np.tile(mrio.sector_inds(), G), 
            'i5': np.tile(mrio.sector_inds(agg=5), G), 
            'i15': np.tile(mrio.sector_inds(agg=15), G),
            'va': mrio.va.data, 
            'y': mrio.Y.row_sum().data,
            'Xv': X.row_sum().data,
            'Xv_D': X_D.row_sum().data,
            'Xv_RT': X_RT.row_sum().data,
            'Xvd_GVC': Xd_GVC.row_sum().data,
            'Ev_GVC': E_GVC.row_sum().data,
            'Xvf_GVC': Xf_GVC.row_sum().data,
            'V_D': VY_D.row_sum().data,
            'V_RT': VY_RT.row_sum().data,
            'V_GVC': VY_GVC.row_sum().data,
            'Xy': X.col_sum().data,
            'Xy_D': X_D.col_sum().data,
            'Xy_RT': X_RT.col_sum().data,
            'Xyd_GVC': Xd_GVC.col_sum().data,
            'Ey_GVC': E_GVC.col_sum().data,
            'Xyf_GVC': Xf_GVC.col_sum().data,
            'Y_D': VY_D.col_sum().data,
            'Y_RT': VY_RT.col_sum().data,
            'Y_GVC': VY_GVC.col_sum().data
        })
        df = pd.concat([df, df_t], ignore_index=True)

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

    apl['PLv_GVC'] = apl['PLvd_GVC'] + apl['CBv_GVC'] + apl['PLvf_GVC']
    apl['PLy_GVC'] = apl['PLyd_GVC'] + apl['CBy_GVC'] + apl['PLyf_GVC']
    apl['GVC_POS'] = apl['PLv_GVC'] / apl['PLy_GVC']
    
    apl = convert_dtypes(apl)
    apl.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')