import numpy as np
import pandas as pd
import duckdb
from functions import subset, asvector, zeroout, diagvec, diagmat, diagrow
import time
np.seterr(divide='ignore', invalid='ignore')

mrio_versions = ['72', '62', '62c']
N, f = 35, 5

for version in mrio_versions:

    start = time.time()

    input, output = f'mrio-{version}.parquet', f'ed-{version}.parquet'
    years = duckdb.sql(f"SELECT DISTINCT t FROM 'data/{input}' ORDER BY t").df()['t']
    nyears = len(years)
    rows = duckdb.sql(f"SELECT count(*) FROM 'data/{input}'").df()
    G = int((rows.iloc[0, 0] / nyears-7) / N)

    ed_es, ed_os = pd.DataFrame(), pd.DataFrame()

    for year in years:
        
        checkpoint_start = time.time()

        mrio = duckdb.sql(f"SELECT * EXCLUDE(t, si) FROM 'data/{input}' WHERE t={year}").df()
        mrio = mrio.values

        x = mrio[-1][:(G*N)]
        Z = mrio[:(G*N)][:, :(G*N)]
        Z1 = Z @ np.kron(np.eye(G, dtype=bool), np.ones((N, 1), dtype=bool))
        va = np.sum(mrio[-7:-1][:, :(G*N)], axis=0)
        Y_big = mrio[:(G*N)][:, (G*N):-1]
        Y = Y_big @ np.kron(np.eye(G, dtype=bool), np.ones((f, 1), dtype=bool))
        Yd, Yf = zeroout(Y, inverse=True), zeroout(Y)

        v = np.where(x != 0, va/x, 0).astype(np.float32)
        A = Z @ np.diag(np.where(x != 0, 1/x, 0))
        Ad, Af = zeroout(A, inverse=True), zeroout(A)
        B = np.linalg.inv(np.eye(G*N, dtype=bool) - A)
        Bd = np.linalg.inv(np.eye(G*N, dtype=bool) - Ad)
        E = zeroout(Z1 + Y).astype(np.float32)

        for s in range(1, G+1):

            Bnots = np.linalg.inv(np.eye(G*N, dtype=np.uint8) - zeroout(A, s, -s))
            
            # Breakdown by export sectors

            Exports = subset(E, s, -s)
            VB_DC = np.diag(subset(v, s) @ subset(Bnots, s, s))
            VB_FC = np.diag(subset(v, -s) @ subset(Bnots, -s, s))

            DAVAX1 = VB_DC @ subset(Y, s, -s)
            DAVAX2 = VB_DC @ (subset(A, s, -s) @ subset(Bd, -s, -s) @ subset(Yd, -s, -s))
            REX1 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(np.sum(subset(Yf, -s, -s), axis=1))
            REX2 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagmat(subset(Af, -s, 0) @ B @ subset(Y, 0, -s), offd=True)
            REX3 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagmat(subset(Af, -s, 0) @ B @ subset(Y, 0, -s))
            REF1 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(subset(Yf, -s, s))
            REF2 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(subset(Af, -s, 0) @ B @ subset(Y, 0, s))
            FVA = VB_FC @ subset(E, s, -s)
            PDC1 = np.diag(subset(v, s) @ subset(Bnots, s, s) @ subset(Af, s, 0) @ subset(B, 0, s)) @ subset(E, s, -s)
            PDC2 = np.diag(subset(v, -s) @ subset(Bnots, -s, s) @ subset(Af, s, 0) @ subset(B, 0, s)) @ subset(E, s, -s)
            
            ed_es_s = pd.DataFrame({
                't': year, 
                's': s, 
                'r': np.setdiff1d(np.arange(1, G+1, dtype=np.uint8), s).repeat(N),
                'breakdown': 'es',
                'i': np.tile(np.arange(1, N+1, dtype=np.uint8), G-1), 
                'exports': asvector(Exports),
                'davax1': asvector(DAVAX1), 'davax2': asvector(DAVAX2),
                'rex1': asvector(REX1), 'rex2': asvector(REX2), 'rex3': asvector(REX3),
                'ref1': asvector(REF1), 'ref2': asvector(REF2),
                'fva': asvector(FVA), 'pdc1': asvector(PDC1), 'pdc2': asvector(PDC2)
            })
            ed_es = pd.concat([ed_es, ed_es_s], ignore_index=True)

            # Breakdown by origin sectors

            VB_DC = np.diag(subset(v, s)) @ subset(Bnots, s, s)
            VB_FC = diagrow(subset(v, -s)) @ subset(Bnots, -s, s)
            
            DAVAX1 = VB_DC @ subset(Y, s, -s)
            DAVAX2 = VB_DC @ (subset(A, s, -s) @ subset(Bd, -s, -s) @ subset(Yd, -s, -s))
            REX1 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(np.sum(subset(Yf, -s, -s), axis=1))
            REX2 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagmat(subset(Af, -s, 0) @ B @ subset(Y, 0, -s), offd=True)
            REX3 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagmat(subset(Af, -s, 0) @ B @ subset(Y, 0, -s))
            REF1 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(subset(Yf, -s, s))
            REF2 = VB_DC @ subset(A, s, -s) @ subset(Bd, -s, -s) @ diagvec(subset(Af, -s, 0) @ B @ subset(Y, 0, s))
            FVA = VB_FC @ subset(E, s, -s)
            PDC1 = VB_DC @ (subset(Af, s, 0) @ subset(B, 0, s) @ subset(E, s, -s))
            PDC2 = VB_FC @ (subset(Af, s, 0) @ subset(B, 0, s) @ subset(E, s, -s))
            
            ed_os_s = pd.DataFrame({
                't': year, 
                's': s, 
                'r': np.setdiff1d(np.arange(1, G+1, dtype=np.uint8), s).repeat(N),
                'breakdown': 'os',
                'i': np.tile(np.arange(1, N+1, dtype=np.uint8), G-1), 
                'exports': np.nan,
                'davax1': asvector(DAVAX1), 'davax2': asvector(DAVAX2),
                'rex1': asvector(REX1), 'rex2': asvector(REX2), 'rex3': asvector(REX3),
                'ref1': asvector(REF1), 'ref2': asvector(REF2),
                'fva': asvector(FVA), 'pdc1': asvector(PDC1), 'pdc2': asvector(PDC2)
            })
            ed_os = pd.concat([ed_os, ed_os_s], ignore_index=True)

        # Time check
        checkpoint_end = time.time()
        elapsed = checkpoint_end - checkpoint_start
        tot_elapsed = checkpoint_end - start
        nyears = nyears-1
        time_elapsed = f'{int(tot_elapsed // 60)} mins {round(tot_elapsed % 60, 1)} secs'
        time_left = f'{int((elapsed * nyears) / 60)} mins'

        print(f'\nMRIO-{version}: {year} done. \nTime elapsed: {time_elapsed}. \nEst. time left: ~{time_left}.')

    ed = pd.concat([ed_es, ed_os], ignore_index=True)

    ed['t'] = ed['t'].astype(np.uint16)
    ed['s'] = ed['s'].astype(np.uint8)
    ed['davax1'] = ed['davax1'].astype(np.float32)
    ed['davax2'] = ed['davax2'].astype(np.float32)
    ed['rex1'] = ed['rex1'].astype(np.float32)
    ed['rex2'] = ed['rex2'].astype(np.float32)
    ed['rex3'] = ed['rex3'].astype(np.float32)
    ed['ref1'] = ed['ref1'].astype(np.float32)
    ed['ref2'] = ed['ref2'].astype(np.float32)
    ed['pdc1'] = ed['pdc1'].astype(np.float32)
    ed['fva'] = ed['fva'].astype(np.float32)
    ed['pdc2'] = ed['pdc2'].astype(np.float32)

    ed.to_parquet(f'data/{output}', index=False)
