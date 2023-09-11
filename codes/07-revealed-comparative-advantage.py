import duckdb

mrio_versions = ['72', '62', '62c']

def table_query(breakdown, index, level):
    return(
        f"""
        SELECT 
            tbl_si.t, tbl_si.s, {level} AS agg, tbl_si.{index} AS i, 
            tbl_si.exports_si, tbl_s.exports_s, tbl_i.exports_i, tbl_all.exports_all,
            tbl_si.vax{breakdown}_si, tbl_s.vax{breakdown}_s, tbl_i.vax{breakdown}_i, tbl_all.vax{breakdown}_all
            
        FROM (
            SELECT t, s, {index}, 
                sum(exports) AS exports_si, 
                sum(davax1 + davax2 + rex1 + rex2 + rex3) AS vax{breakdown}_si
            FROM ed WHERE breakdown='{breakdown}' GROUP BY t, s, {index}
        ) AS tbl_si

        JOIN (
            SELECT t, s, 
                sum(exports) AS exports_s, 
                sum(davax1 + davax2 + rex1 + rex2 + rex3) AS vax{breakdown}_s
            FROM ed WHERE breakdown='{breakdown}' GROUP BY t, s
        ) AS tbl_s
        ON tbl_si.t = tbl_s.t AND tbl_si.s = tbl_s.s

        JOIN (
            SELECT t, {index}, 
                sum(exports) AS exports_i, 
                sum(davax1 + davax2 + rex1 + rex2 + rex3) AS vax{breakdown}_i
            FROM ed WHERE breakdown='{breakdown}' GROUP BY t, {index}
        ) AS tbl_i
        ON tbl_si.t = tbl_i.t AND tbl_si.{index} = tbl_i.{index}

        JOIN (
            SELECT t, 
                sum(exports) AS exports_all, 
                sum(davax1 + davax2 + rex1 + rex2 + rex3) AS vax{breakdown}_all
            FROM ed WHERE breakdown='{breakdown}' GROUP BY t
        ) AS tbl_all
        ON tbl_si.t = tbl_all.t

        GROUP BY 
            tbl_si.t, tbl_si.s, tbl_si.{index}, 
            tbl_si.exports_si, tbl_s.exports_s, tbl_i.exports_i, tbl_all.exports_all,
            tbl_si.vax{breakdown}_si, tbl_s.vax{breakdown}_s, tbl_i.vax{breakdown}_i, tbl_all.vax{breakdown}_all
        
        ORDER BY tbl_si.t, tbl_si.s, tbl_si.{index}
        """
    )

for version in mrio_versions:

    ed, output = f'ed-{version}.parquet', f'rca-{version}.parquet'

    rca = duckdb.sql(
        f"""
        WITH ed AS (
            SELECT *
            FROM (SELECT * FROM 'data/{ed}') AS ed
            JOIN (SELECT * FROM read_csv_auto('dicts/sectors.csv')) AS dict
            ON ed.i = dict.c_ind
        )

        SELECT tbl_es.t, tbl_es.s, tbl_es.agg, tbl_es.i, 
            exports_si, exports_s, exports_i, exports_all, 
            vaxes_si, vaxes_s, vaxes_i, vaxes_all, 
            vaxos_si, vaxos_s, vaxos_i, vaxos_all,
            (exports_si / exports_s) / (exports_i / exports_all) AS rca,
            (vaxes_si / vaxes_s) / (vaxes_i / vaxes_all) AS rca_vaxes,
            (vaxos_si / vaxos_s) / (vaxos_i / vaxos_all) AS rca_vaxos

        FROM (
            ( {table_query('es', 'c5_ind', '0')} )
            UNION ALL ( {table_query('es', 'c5_ind', '5')} )
            UNION ALL ( {table_query('es', 'c15_ind', '15')} )
            UNION ALL ( {table_query('es', 'c_ind', '35')} )
        ) AS tbl_es

        JOIN (
            SELECT * EXCLUDE(exports_si, exports_s, exports_i, exports_all)
            FROM (
                ( {table_query('os', 'c5_ind', '0')} )
                UNION ALL ( {table_query('os', 'c5_ind', '5')} )
                UNION ALL ( {table_query('os', 'c15_ind', '15')} )
                UNION ALL ( {table_query('os', 'c_ind', '35')} )
            )
        ) AS tbl_os
        ON tbl_es.t=tbl_os.t AND tbl_es.s=tbl_os.s AND tbl_es.agg=tbl_os.agg AND tbl_es.i=tbl_os.i

        ORDER BY tbl_es.t, tbl_es.agg, tbl_es.s, tbl_es.i
        """
    ).df()

    rca.to_parquet(f'data/{output}.parquet', index=False)