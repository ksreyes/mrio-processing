import pandas as pd
import duckdb

mrio_versions = ['72', '62', '62c']

for version in mrio_versions:

    ed, flows = f'ed-{version}.parquet', f'flows-{version}.parquet'
    output = f'rci-{version}.parquet'
    
    if version == '72':
        index = 'mrio'
    else:
        index = 'mrio62'

    grossexports = duckdb.sql(
        f"""
        SELECT t, s, r, sum(exports) AS exports
        FROM 'data/{ed}'
        WHERE breakdown='es'
        GROUP BY t, s, r
        ORDER BY t, s, r
        """
    ).df()

    intlflows = duckdb.sql(
        f"""
        SELECT t, s, r, SUM(va_flows) AS flows
        FROM 'data/{flows}'
        WHERE s<>r
        GROUP BY t, s, r
        ORDER BY t, s, r
        """
    ).df()

    countries = pd.read_excel('dicts/countries.xlsx')
    rta = countries[[f'{index}', 'rta_asean', 'rta_carec', 'rta_carec10', 'rta_eaeu', 'rta_eu', 'rta_nafta', 'rta_safta']]
    rta = rta.fillna(0)

    def compute_rci(method):

        if (method == 'gross exports'):
            input, name, flow = 'grossexports', 'gross exports', 'exports'

        else: 
            input, name, flow = 'intlflows', 'end-to-end', 'flows'
        
        world = duckdb.sql(f'SELECT t, sum({flow}) AS world FROM {input} GROUP BY t').df()

        exports = duckdb.sql(
            f'''
            SELECT t, rta, sum({flow}) AS exports
            FROM (
                SELECT *
                FROM (
                    UNPIVOT (
                        SELECT * EXCLUDE (r, {index})
                        FROM (SELECT * FROM {input}) AS tbl
                        JOIN (SELECT * FROM rta) AS rtas ON tbl.s=rtas.{index}
                    ) AS tbl_long
                    ON COLUMNS(* EXCLUDE (t, s, {flow}))
                    INTO NAME rta VALUE member
                )
                WHERE member=1
            )
            GROUP BY t, rta
            '''
        ).df()

        imports = duckdb.sql(
            f"""
            SELECT t, rta, sum({flow}) AS imports
            FROM (
                SELECT *
                FROM (
                    UNPIVOT (
                        SELECT * EXCLUDE (r, {index})
                        FROM (SELECT * FROM {input}) AS tbl
                        JOIN (SELECT * FROM rta) AS rtas ON tbl.r=rtas.{index}
                    ) AS tbl_long
                    ON COLUMNS(* EXCLUDE (t, s, {flow}))
                    INTO NAME rta VALUE member
                )
                WHERE member=1
            )
            GROUP BY t, rta
            """
        ).df()

        within = duckdb.sql(
            f"""
            WITH aggregate1 AS (
                SELECT * EXCLUDE (member)
                FROM (
                    UNPIVOT (
                        SELECT * EXCLUDE ({index})
                        FROM (SELECT * FROM {input}) AS tbl
                        JOIN (SELECT * FROM rta) AS rtas ON tbl.s = rtas.{index}
                    ) AS tbl_long
                    ON COLUMNS(* EXCLUDE (t, s, r, {flow}))
                    INTO NAME exporter VALUE member
                )
                WHERE member=1
            ), 

            aggregate AS (
                SELECT * EXCLUDE (member)
                FROM (
                    UNPIVOT (
                        SELECT * EXCLUDE ({index})
                        FROM (SELECT * FROM aggregate1) AS tbl
                        JOIN (SELECT * FROM rta) AS rtas ON tbl.r = rtas.{index}
                    ) AS tbl_long
                    ON COLUMNS(* EXCLUDE (t, s, r, {flow}, exporter))
                    INTO NAME importer VALUE member
                )
                WHERE member=1
            )

            SELECT t, exporter AS rta, sum({flow}) AS within 
            FROM (SELECT * FROM aggregate WHERE exporter=importer)
            GROUP BY t, exporter, importer
            """
        ).df()

        df = duckdb.sql(
            f"""
            SELECT 
                t, '{name}' AS method, rta, within, exports, imports, world, 
                (2 * within) / (exports + imports) AS share_within, 
                (exports + imports) / (2 * world) AS share_world,
                share_within / share_world AS rci
            FROM (
                SELECT * FROM within 
                JOIN exports ON within.t=exports.t AND within.rta=exports.rta
                JOIN imports ON within.t=imports.t AND within.rta=imports.rta
                JOIN world ON within.t=world.t
            )
            ORDER BY rta, t
            """
        ).df()

        df['rta'] = df['rta'].str.split('_').str[-1]
        return(df)

    rci = pd.concat([
            compute_rci(method='gross exports'), 
            compute_rci(method='end-to-end')
        ]).reset_index(drop=True)

    rci.to_parquet(f'data/{output}', index=False)