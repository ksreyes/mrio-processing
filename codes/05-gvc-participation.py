import duckdb
from utils import aggregate_sectors, convert_dtypes

mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    ed, summary = f'ed-{version}.parquet', f'summary-{version}.parquet'
    output = f'gvcp-{version}.parquet'

    df_es = aggregate_sectors(
        table = f"'data/{ed}'",
        filter = "breakdown='es'",
        cols_index = ['t', 's'],
        cols_to_sum = ['exports', 'davax1', 'davax2', 'rex1', 'rex2', 'rex3', 'ref1', 'ref2', 'fva', 'pdc1', 'pdc2']
    )

    df_es = duckdb.sql(
        '''
        SELECT t, s, agg, i, exports, 
            rex1 + rex2 + rex3 + ref1 + ref2 AS gvc_trade_f,
            fva + pdc1 + pdc2 AS gvc_trade_b,
            rex1 + rex2 + rex3 + ref1 + ref2 + fva + pdc1 + pdc2 AS gvc_trade
        FROM df_es
        '''
    ).df()

    df_os = aggregate_sectors(
        table = f"'data/{ed}'",
        filter = "breakdown='os'",
        cols_index = ['t', 's'],
        cols_to_sum = ['davax1', 'davax2', 'rex1', 'rex2', 'rex3', 'ref1', 'ref2', 'fva', 'pdc1', 'pdc2']
    )

    df_os = duckdb.sql(
        f"""
        SELECT df_os.t, df_os.s, df_os.agg, df_os.i, va,
            davax2 + rex1 + rex2 + rex3 + ref1 + ref2 AS gvc_prod
        FROM (SELECT * FROM df_os) AS df_os
        JOIN (SELECT * FROM 'data/{summary}') AS summary
        ON df_os.t = summary.t AND df_os.s = summary.s AND df_os.agg = summary.agg AND df_os.i = summary.i 
        """
    ).df()

    gvcp = duckdb.sql(
        '''
        SELECT df_es.t, df_es.s, df_es.agg, df_es.i,
            exports, va, gvc_trade_f, gvc_trade_b, gvc_trade, gvc_prod,
            gvc_trade_f / exports AS gvcp_trade_f,
            gvc_trade_b / exports AS gvcp_trade_b,
            gvc_trade / exports AS gvcp_trade,
            gvc_prod / va AS gvcp_prod
        FROM (SELECT * FROM df_es) AS df_es
        JOIN (SELECT * FROM df_os) AS df_os
        ON df_es.t = df_os.t AND df_es.s = df_os.s AND df_es.agg = df_os.agg AND df_es.i = df_os.i
        ORDER BY df_es.t, df_es.agg, df_es.s, df_es.i
        '''
    ).df()

    gvcp = convert_dtypes(gvcp)
    gvcp.to_parquet(f'data/{output}', index=False)