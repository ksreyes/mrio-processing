### Choose MRIO version #######################################################
inputfolder, version = 'ADB MRIO, 72 economies', '72'
# inputfolder, version = 'ADB MRIO, 62 economies', '62'
# inputfolder, version = 'ADB MRIO constant price', '62c'
###############################################################################

import numpy as np
import pandas as pd
import os
import re

output = f'mrio-{version}.parquet'
mrio = pd.DataFrame()

filelist = [file for file in os.listdir(f'data/raw/{inputfolder}') if not file.startswith(('~$', '.'))]
filelist.sort()

for file in filelist:

    year = re.search('[0-9]{4}', file).group()
    mrio_t = pd.read_excel(f'data/raw/{inputfolder}/{file}', skiprows=5, header=[0,1])

    # Deduce parameters
    mrio_t = mrio_t.drop(mrio_t.index[-1])
    N = 35
    f = 5
    G = int((mrio_t.shape[0] - 8) / N)

    # Remove all irrelevant columns
    mrio_t = mrio_t.iloc[:, 2:(5 + G*N + G*f)]

    # Collapse MultiIndex headers into one
    mrio_t.columns = [f'{level_1}_{level_2}' for level_1, level_2 in mrio_t.columns]

    # Rename the ToT column
    colnames = mrio_t.columns.tolist()
    mapping = {colnames[-1]: 'ToT'}
    mrio_t = mrio_t.rename(columns=mapping)

    # Fix row labels
    rowlabels = [f"{c}_{d}" if not (pd.isna(c) or c == 'ToT') else d for c, d in zip(mrio_t.iloc[:, 0], mrio_t.iloc[:, 1])]
    mrio_t.insert(2, 'si', rowlabels)
    mrio_t = mrio_t.iloc[:, 2:]

    # Drop intermediates totals
    mrio_t = mrio_t.drop(mrio_t[mrio_t['si'] == 'r60'].index)

    # Replace blank cells with zero
    mrio_t = mrio_t.replace(' ', 0)

    mrio_t.insert(0, 't', year)
    mrio_t['t'] = mrio_t['t'].astype(np.uint16)
    mrio = pd.concat([mrio, mrio_t], ignore_index=True)

    print(f'{year} done')

mrio.to_parquet(f'data/{output}', index=False)