import pandas as pd
import duckdb
import time
from mrio import MRIO
from utils import get_years, ind_pattern, aggregate_sectors, convert_dtypes, progress_check

start = time.time()
mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    df = pd.DataFrame()
    input = f'mrio-{version}.parquet'
    output = f'apl-{version}.parquet'
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
            's': ind_pattern(mrio.country_inds(), repeat=N),
            'i': ind_pattern(mrio.sector_inds(), tile=G), 
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

    apl = aggregate_sectors(
        table = 'df',
        cols_index = ['t', 's'],
        cols_to_sum = [
            'va', 'y',
            'Xv', 'Xv_D', 'Xv_RT', 'Xvd_GVC', 'Ev_GVC', 'Xvf_GVC', 'V_D', 'V_RT', 'V_GVC',
            'Xy', 'Xy_D', 'Xy_RT', 'Xyd_GVC', 'Ey_GVC', 'Xyf_GVC', 'Y_D', 'Y_RT', 'Y_GVC'
        ])

    apl = duckdb.sql(
        '''
        SELECT t, s, agg, i, 
            Xv / va          AS  PLv, 
            Xv_D / V_D       AS  PLv_D, 
            Xv_RT / V_RT     AS  PLv_RT, 
            Xvd_GVC / V_GVC  AS  PLvd_GVC, 
            Ev_GVC / V_GVC   AS  CBv_GVC, 
            Xvf_GVC / V_GVC  AS  PLvf_GVC, 
            Xy / y           AS  PLy, 
            Xy_D / Y_D       AS  PLy_D, 
            Xy_RT / Y_RT     AS  PLy_RT, 
            Xyd_GVC / Y_GVC  AS  PLyd_GVC, 
            Ey_GVC / Y_GVC   AS  CBy_GVC, 
            Xyf_GVC / Y_GVC  AS  PLyf_GVC
        FROM apl
        '''
    ).df()

    apl = duckdb.sql(
        '''
        SELECT *, 
            PLvd_GVC + CBv_GVC + PLvf_GVC        AS  PLv_GVC,
            PLyd_GVC + CBy_GVC + PLyf_GVC        AS  PLy_GVC,
            (PLvd_GVC + CBv_GVC + PLvf_GVC) / 
                (PLyd_GVC + CBy_GVC + PLyf_GVC)  AS  GVC_POS
        FROM apl
        '''
    ).df()
    
    apl = convert_dtypes(apl)
    apl.to_parquet(f'data/{output}', index=False)

    progress_check(start, version)

print('')