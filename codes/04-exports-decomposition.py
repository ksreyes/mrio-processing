import numpy as np
import pandas as pd
import time
from mrio import MRIO, progress_check, get_years, convert_dtypes

start = time.time()
# mrio_versions = ['72', '62', '62c']
mrio_versions = ['62']

for version in mrio_versions:

    ed_es, ed_os = pd.DataFrame(), pd.DataFrame()
    input, output = f'mrio-{version}.parquet', f'ed-{version}.parquet'
    years = get_years(f'data/{input}')

    for year in years:
        
        checkpoint_start = time.time()

        mrio = MRIO(f'data/{input}', year, full=True)
        G, N = mrio.G, mrio.N

        v = mrio.v
        Y = mrio.Y
        Yd = mrio.Y.zeroout(inverse=True)
        Yf = mrio.Y.zeroout()
        A = mrio.A
        Ad = mrio.A.zeroout(inverse=True)
        Af = mrio.A.zeroout()
        B = mrio.B
        Bd = (mrio.I(G*N) - Ad).invert()
        E = mrio.Z.row_sum(chunk=N) + mrio.Y

        for s in range(1, G+1):
            
            Bnots = (mrio.I(G*N) - A.zeroout(s, -s)).invert()
            
            # Breakdown by export sectors

            Exports = E.subset(s, -s)
            VB_DC = (v.subset(s) @ Bnots.subset(s, s)).diag()
            VB_FC = (v.subset(-s) @ Bnots.subset(-s, s)).diag()
            DAVAX1 = VB_DC @ Y.subset(s, -s)
            DAVAX2 = VB_DC @ (A.subset(s, -s) @ Bd.subset(-s, -s) @ Yd.subset(-s, -s))
            REX1 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ Yf.subset(-s, -s).row_sum().diagvec()
            REX2 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, -s)).diagmat(offd=True)
            REX3 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, -s)).diagmat()
            REF1 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ Yf.subset(-s, s).diagvec()
            REF2 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, s)).diagvec()
            FVA = VB_FC @ E.subset(s, -s)
            PDC1 = (v.subset(s) @ Bnots.subset(s, s) @ Af.subset(s, 0) @ B.subset(0, s)).diag() @ E.subset(s, -s)
            PDC2 = (v.subset(-s) @ Bnots.subset(-s, s) @ Af.subset(s, 0) @ B.subset(0, s)).diag() @ E.subset(s, -s)
            
            ed_es_s = pd.DataFrame({
                't': year, 
                's': s, 
                'r': mrio.country_inds(exclude=s).repeat(N),
                'breakdown': 'es',
                'i': np.tile(mrio.sector_inds(), G-1), 
                'exports': Exports.asvector().data,
                'davax1': DAVAX1.asvector().data,
                'davax2': DAVAX2.asvector().data,
                'rex1': REX1.asvector().data,
                'rex2': REX2.asvector().data,
                'rex3': REX3.asvector().data,
                'ref1': REF1.asvector().data,
                'ref2': REF2.asvector().data,
                'fva': FVA.asvector().data,
                'pdc1': PDC1.asvector().data,
                'pdc2': PDC2.asvector().data
            })
            ed_es = pd.concat([ed_es, ed_es_s], ignore_index=True)

            # Breakdown by origin sectors

            VB_DC = v.subset(s).diag() @ Bnots.subset(s, s)
            VB_FC = v.subset(-s).diagrow() @ Bnots.subset(-s, s)
            DAVAX1 = VB_DC @ Y.subset(s, -s)
            DAVAX2 = VB_DC @ (A.subset(s, -s) @ Bd.subset(-s, -s) @ Yd.subset(-s, -s))
            REX1 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ Yf.subset(-s, -s).row_sum().diagvec()
            REX2 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, -s)).diagmat(offd=True)
            REX3 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, -s)).diagmat()
            REF1 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ Yf.subset(-s, s).diagvec()
            REF2 = VB_DC @ A.subset(s, -s) @ Bd.subset(-s, -s) @ (Af.subset(-s, 0) @ B @ Y.subset(0, s)).diagvec()
            FVA = VB_FC @ E.subset(s, -s)
            PDC1 = VB_DC @ (Af.subset(s, 0) @ B.subset(0, s) @ E.subset(s, -s))
            PDC2 = VB_FC @ (Af.subset(s, 0) @ B.subset(0, s) @ E.subset(s, -s))
            
            ed_os_s = pd.DataFrame({
                't': year, 
                's': s, 
                'r': mrio.country_inds(exclude=s).repeat(N),
                'breakdown': 'os',
                'i': np.tile(mrio.sector_inds(), G-1), 
                'exports': np.nan,
                'davax1': DAVAX1.asvector().data,
                'davax2': DAVAX2.asvector().data,
                'rex1': REX1.asvector().data,
                'rex2': REX2.asvector().data,
                'rex3': REX3.asvector().data,
                'ref1': REF1.asvector().data,
                'ref2': REF2.asvector().data,
                'fva': FVA.asvector().data,
                'pdc1': PDC1.asvector().data,
                'pdc2': PDC2.asvector().data
            })
            ed_os = pd.concat([ed_os, ed_os_s], ignore_index=True)

        # Time check
        # checkpoint_end = time.time()
        # elapsed = checkpoint_end - checkpoint_start
        # tot_elapsed = checkpoint_end - start
        # time_elapsed = f'{int(tot_elapsed // 60)} mins {round(tot_elapsed % 60, 1)} secs'

        # print(f'\nMRIO-{version}: {year} done. \nTime elapsed: {time_elapsed}.')

        progress_check(start, version, year)

    ed = pd.concat([ed_es, ed_os], ignore_index=True)
    ed = convert_dtypes(ed)
    ed.to_parquet(f'data/{output}', index=False)

print('')