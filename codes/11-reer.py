import numpy as np
import pandas as pd
import duckdb

deflators = 'deflators-62.parquet'
weights = 'reer-62-weights'
output = 'reer-62.parquet'

df1 = duckdb.sql(f"SELECT * FROM 'data/reer/{deflators}' WHERE inflation IS NOT NULL").df()
df2 = duckdb.sql(f"SELECT * FROM 'data/reer/{weights}.parquet'").df()

years = df1['t'].unique()
G = df1['s'].max()
N = df1['i'].max()
methods = df2['method'].unique()

reer_agg = pd.DataFrame()

for method in methods:
    
    for year in years:
        
        weights_mt = duckdb.sql(
            f"""
            SELECT * EXCLUDE(method, t)
            FROM 'data/reer/{weights}.parquet'
            WHERE method='{method}' AND t={year}
            """
        ).df()

        deflators_t = duckdb.sql(
            f"""
            SELECT * 
            FROM 'data/reer/{deflators}'
            WHERE agg=0 AND t={year}
            """
        ).df()

        reer_t = pd.DataFrame({
            'method': method,
            't': int(year), 's': np.arange(1, G+1), 
            'agg': 0, 'i': 0,
            'reer': weights_mt @ np.array(deflators_t['inflation'])
        })

        reer_agg = pd.concat([reer_agg, reer_t], ignore_index=True)

reer_sector = pd.DataFrame()

for year in years:
    
    weights_t = duckdb.sql(
        f"""
        SELECT * EXCLUDE(t)
        FROM 'data/reer/{weights}-sector.parquet'
        WHERE t={year}
        """
    ).df()

    deflators_t = duckdb.sql(
        f"""
        SELECT * 
        FROM 'data/reer/{deflators}'
        WHERE agg=35 AND t={year}
        """
    ).df()
    
    reer_t = pd.DataFrame({
        'method': 'pww-sector',
        't': year, 's': np.arange(1, G+1).repeat(N),
        'agg': 35, 'i': np.tile(np.arange(1, N+1), G),
        'reer': weights_t @ np.array(deflators_t['inflation'])
    })

    reer_sector = pd.concat([reer_sector, reer_t], ignore_index=True)

reer = pd.concat([reer_agg, reer_sector]).dropna().reset_index(drop=True)
reer.to_parquet(f'data/reer/{output}', index=False)