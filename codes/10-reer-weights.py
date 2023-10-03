import pandas as pd
import time
from mrio import MRIO
from utils import get_years, progress_check

start = time.time()
mrio_versions = ['72', '62']

def to_df(matrix, year, method=None):
    df = pd.DataFrame(matrix)
    df.insert(0, 't', int(year))
    if method is not None:
        df.insert(0, 'method', method)
    return df

for version in mrio_versions:

    DF, DF_sector = pd.DataFrame(), pd.DataFrame()
    input, output = f'mrio-{version}', f'reer-{version}-weights'
    years = get_years(f'data/{input}.parquet')

    for year in years:

        mrio = MRIO(f'data/{input}.parquet', year, full=True)
        G, N = mrio.G, mrio.N
        
        y = mrio.Y.col_sum()
        A1 = (1/mrio.x).diag() @ mrio.Z
        Leontief = (mrio.I(G*N) - mrio.A).invert()
        Ghosh = (mrio.I(G*N) - A1).invert()
        
        agg = mrio.I(G).kron(mrio.i((N, 1)))
        Z_agg = agg.t() @ mrio.Z @ agg
        Y_agg = agg.t() @ mrio.Y
        D_agg = Z_agg + Y_agg
        va_agg = mrio.va @ agg

        z_agg = Z_agg.col_sum()
        d_agg = D_agg.col_sum()
        x_agg = z_agg + va_agg
        e_agg = z_agg + y

        A_agg = Z_agg @ (1/x_agg).diag()
        B_agg = (1/x_agg).diag() @ Z_agg
        Leontief_agg = (mrio.I(G) - A_agg).invert()
        Ghosh_agg = (mrio.I(G) - B_agg).invert()

        # Weights matrices: Conventional

        S = (1/d_agg).diag() @ D_agg.t()
        W = (1/e_agg).diag() @ D_agg
        SW0 = S @ W * (1 - mrio.I(G))
        Weights = mrio.I(G) - (1 / SW0.row_sum()).diag() @ SW0

        # Weights matrices: Bems & Johnson

        Sy = (1/x_agg).diag() @ Y_agg
        Sz = (1/x_agg).diag() @ Z_agg
        Wy = (1/y).diag() @ Y_agg.t()
        Wz = (1/z_agg).diag() @ Z_agg.t()
        v_agg = (va_agg/x_agg).diag()
        SW0_BJ = Ghosh_agg @ Sy @ Wy @ Leontief_agg.t() @ v_agg * (1 - mrio.I(G))
        Weights_BJ = mrio.I(G) - (1 / SW0_BJ.row_sum()).diag() @ SW0_BJ

        # Weights matrices: Patel, Wang, & Wei

        SSy = (1/mrio.x).diag() @ mrio.Y
        WWy = (1/y).diag() @ mrio.Y.t()
        SW0_PWW = Ghosh @ SSy @ WWy @ Leontief.t() @ mrio.v.diag() * (1 - mrio.I(G*N))
        Shares_PWW = (1 / SW0_PWW.row_sum()).diag()
        Weights_PWW = mrio.I(G*N) - Shares_PWW @ SW0_PWW

        R = (1/va_agg).diag() @ mrio.i((G, 1)).kron(mrio.va) * mrio.I(G).kron(mrio.i((N, 1))).t()
        SW0_PWW_agg = R @ (Ghosh @ SSy @ WWy @ Leontief.t() @ mrio.v.diag()) @ agg * (1 - mrio.I(G))
        Shares_PWW_agg = (1 / SW0_PWW_agg.row_sum()).diag()
        Weights_PWW_agg = mrio.I(G) - Shares_PWW_agg @ SW0_PWW_agg
        
        DF = pd.concat([
                DF,
                to_df(Weights.data, year, 'conventional'), 
                to_df(Weights_BJ.data, year, 'bj'), 
                to_df(Weights_PWW_agg.data, year, 'pww')
            ],
            ignore_index=True
        )
        DF_sector = pd.concat([DF_sector, to_df(Weights_PWW.data, year)], ignore_index=True)

    DF.to_parquet(f'data/reer/{output}.parquet', index=False)
    DF_sector.to_parquet(f'data/reer/{output}-sector.parquet', index=False)

    progress_check(start, version)

print('')