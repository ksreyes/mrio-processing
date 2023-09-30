import pandas as pd
import duckdb

input = 'ADB-MRIO-Deflators_with2022.xlsx'
summary = 'summary-62.parquet'
output = 'deflators-62.parquet'

# Load and clean
deflators = pd.read_excel(
    f'data/raw/{input}',
    sheet_name='Production',
    skiprows=[0,1,3,4],
    header=[0,1]
)
deflators.insert(0, 's', deflators.index+1)
deflators = pd.melt(deflators, id_vars=['s', ('Year', 'Country Code')])
deflators.columns = ['s', 'code', 't', 'i', 'deflator']
deflators['i'] = deflators['i'].str.replace(r'c', '').astype(int)
deflators['t'] = deflators['t'].astype(int)

# Load country-sector value added
va = duckdb.sql(f"SELECT t, s, i, va FROM 'data/{summary}'").df()
va_sum = va.groupby(['s', 't'])['va'].sum().reset_index()
va_sum.rename(columns={'va': 'va_sum'}, inplace=True)
va = pd.merge(va, va_sum, on=['s', 't'])
va['va_sh'] = va['va'] / va['va_sum']

deflators = pd.merge(deflators, va)

# Compute aggregate deflator
deflators['def_va_sh'] = deflators['deflator'] * deflators['va_sh']
deflators_agg = deflators.groupby(['t', 's'])['def_va_sh'].sum().reset_index()
deflators_agg.rename(columns={'def_va_sh': 'deflator'}, inplace=True)
deflators_agg['agg'] = 0
deflators_agg['i'] = 0

# Consolidate
deflators['agg'] = 35
deflators.drop(['code', 'va', 'va_sum', 'va_sh', 'def_va_sh'], axis=1, inplace=True)
deflators = pd.concat([deflators, deflators_agg])
deflators = deflators[['s', 'agg', 'i', 't', 'deflator']]
deflators.sort_values(by=['agg', 's', 'i', 't'], inplace=True)

# Compute inflation
deflators['inflation'] = deflators.groupby(['agg', 's', 'i'])['deflator'].pct_change()
deflators.loc[deflators['t'] == 2007, 'inflation'] = deflators.loc[deflators['t'] == 2007, 'inflation'] / 7

deflators.to_parquet(f'data/reer/{output}', index=False)