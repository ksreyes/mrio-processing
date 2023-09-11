import numpy as np
import duckdb

mrio_versions = ['72', '62', '62c']
N, f = 35, 5
sectors = duckdb.sql("SELECT * FROM read_csv_auto('dicts/sectors.csv')").df()

for version in mrio_versions:

    ed, summary, output = f'ed-{version}.parquet', f'summary-{version}.parquet', f'gvcp-{version}.parquet'

    sum_terms = '''
        sum(davax1) AS davax1,
        sum(davax2) AS davax2,
        sum(rex1) AS rex1,
        sum(rex2) AS rex2,
        sum(rex3) AS rex3,
        sum(ref1) AS ref1,
        sum(ref2) AS ref2,
        sum(fva) AS fva,
        sum(pdc1) AS pdc1,
        sum(pdc2) AS pdc2
        '''

    df_es = duckdb.sql(
        f"""
        WITH ed AS (
            SELECT *
            FROM (SELECT * FROM 'data/{ed}') AS ed
            JOIN (SELECT * FROM sectors) AS dict
            ON ed.i = dict.c_ind
        )

        (SELECT t, s, 0 AS agg, 0 AS i, sum(exports) AS exports, {sum_terms}
        FROM ed WHERE breakdown='es' GROUP BY t, s ORDER BY t, s)

        UNION ALL

        (SELECT t, s, 5 AS agg, c5_ind AS i, sum(exports) AS exports, {sum_terms}
        FROM ed WHERE breakdown='es' GROUP BY t, s, c5_ind ORDER BY t, s, c5_ind)

        UNION ALL
        
        (SELECT t, s, 15 AS agg, c15_ind AS i, sum(exports) AS exports, {sum_terms}
        FROM ed WHERE breakdown='es' GROUP BY t, s, c15_ind ORDER BY t, s, c15_ind)
        
        UNION ALL
        
        (SELECT t, s, 35 AS agg, c_ind AS i, sum(exports) AS exports, {sum_terms}
        FROM ed WHERE breakdown='es' GROUP BY t, s, c_ind ORDER BY t, s, c_ind)
        """
    ).df()

    df_es = duckdb.sql(
        """
        SELECT t, s, agg, i, exports, 
            rex1 + rex2 + rex3 + ref1 + ref2 AS gvc_trade_f,
            fva + pdc1 + pdc2 AS gvc_trade_b,
            rex1 + rex2 + rex3 + ref1 + ref2 + fva + pdc1 + pdc2 AS gvc_trade
        FROM df_es
        """
    ).df()

    df_os = duckdb.sql(
        f"""
        WITH ed AS (
            SELECT *
            FROM (SELECT * FROM 'data/{ed}') AS ed
            JOIN (SELECT * FROM sectors) AS dict
            ON ed.i = dict.c_ind
        )

        (SELECT t, s, 0 AS agg, 0 AS i, {sum_terms}
        FROM ed WHERE breakdown='os' GROUP BY t, s ORDER BY t, s)

        UNION ALL

        (SELECT t, s, 5 AS agg, c5_ind AS i, {sum_terms}
        FROM ed WHERE breakdown='os' GROUP BY t, s, c5_ind ORDER BY t, s, c5_ind)

        UNION ALL
        
        (SELECT t, s, 15 AS agg, c15_ind AS i, {sum_terms}
        FROM ed WHERE breakdown='os' GROUP BY t, s, c15_ind ORDER BY t, s, c15_ind)
        
        UNION ALL
        
        (SELECT t, s, 35 AS agg, c_ind AS i, {sum_terms}
        FROM ed WHERE breakdown='os' GROUP BY t, s, c_ind ORDER BY t, s, c_ind)
        """
    ).df()

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
        f"""
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
        """
    ).df()

    gvcp['t'] = gvcp['t'].astype(np.uint16)
    gvcp['s'] = gvcp['s'].astype(np.uint8)
    gvcp['agg'] = gvcp['agg'].astype(np.uint8)
    gvcp['i'] = gvcp['i'].astype(np.uint8)
    gvcp['exports'] = gvcp['exports'].astype(np.float32)
    gvcp['va'] = gvcp['va'].astype(np.float32)
    gvcp['gvc_trade_f'] = gvcp['gvc_trade_f'].astype(np.float32)
    gvcp['gvc_trade_b'] = gvcp['gvc_trade_b'].astype(np.float32)
    gvcp['gvc_trade'] = gvcp['gvc_trade'].astype(np.float32)
    gvcp['gvc_prod'] = gvcp['gvc_prod'].astype(np.float32)
    gvcp['gvcp_trade_f'] = gvcp['gvcp_trade_f'].astype(np.float32)
    gvcp['gvcp_trade_b'] = gvcp['gvcp_trade_b'].astype(np.float32)
    gvcp['gvcp_trade'] = gvcp['gvcp_trade'].astype(np.float32)
    gvcp['gvcp_prod'] = gvcp['gvcp_prod'].astype(np.float32)

    gvcp.to_parquet(f'data/{output}', index=False)
