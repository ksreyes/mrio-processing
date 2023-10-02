import numpy as np
import pandas as pd
import duckdb
import time

def get_years(file_path):
    years = duckdb.sql(f"SELECT DISTINCT t FROM '{file_path}' ORDER BY t").df()['t']
    return years.values

def progress_check(start_time, mrio_version, current_year=None):

    checkpoint = time.time()
    elapsed = checkpoint - start_time
    mins = int(elapsed // 60)
    secs = round(elapsed % 60, 1)

    if mins == 0:
        time_elapsed = f'Time elapsed: {secs} secs.'
    else:
        time_elapsed = f'Time elapsed: {mins} mins {secs} secs.'

    if current_year is not None:
        status = f'MRIO-{mrio_version}: {current_year} done.'
    else:
        status = f'MRIO-{mrio_version} done.'
    
    print(f'\n{status}\n{time_elapsed}')

def convert_dtypes(dataframe):

    for col in dataframe.columns:

        if dataframe[col].dtypes == 'object':
            continue

        min = dataframe[col].min()
        max = dataframe[col].max()

        if 0 <= min and max <= 255 and min % 1 == 0 and max % 1 == 0:
            dataframe[col] = dataframe[col].astype(np.uint8)
        elif 255 < min and max <= 65535 and min % 1 == 0 and max % 1 == 0:
            dataframe[col] = dataframe[col].astype(np.uint16)
        else:
            dataframe[col] = dataframe[col].astype(np.float32)

    return dataframe

def aggregate_sectors(table, cols_index, cols_to_sum, filter='', N=35):

    dict_sectors = pd.DataFrame({
        'i': np.arange(1, N+1, dtype=np.uint8),
        'i5': np.array([1, 1, 2, 2, 2, 2, 2, 3, 3, 2, 3, 3, 3, 3, 3, 2, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5], dtype=np.uint8),
        'i15': np.array([1, 2, 3, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4, 4, 4, 3, 5, 6, 7, 7, 7, 8, 9, 9, 9, 9, 10, 11, 12, 12, 13, 14, 14, 15, 15], dtype=np.uint8)
    })
    
    for i in range(len(cols_to_sum)):
        cols_to_sum[i] = f'sum({cols_to_sum[i]}) AS {cols_to_sum[i]}'
    
    cols_index = ', '.join(cols_index)
    cols_to_sum = ', '.join(cols_to_sum)

    if filter!='':
        filter = f'WHERE {filter}'

    return duckdb.sql(
        f"""
        WITH df_mapped AS (
            SELECT *
            FROM (SELECT * FROM {table} {filter}) AS df1
            JOIN (SELECT * FROM dict_sectors) AS dict
            ON df1.i = dict.i
        )

        (SELECT {cols_index}, 0 AS agg, 0 AS i, {cols_to_sum}
         FROM df_mapped GROUP BY {cols_index} ORDER BY {cols_index})

        UNION ALL

        (SELECT {cols_index}, 5 AS agg, i5 AS i, {cols_to_sum}
         FROM df_mapped GROUP BY {cols_index}, i5 ORDER BY {cols_index}, i5)

        UNION ALL

        (SELECT {cols_index}, 15 AS agg, i15 AS i, {cols_to_sum}
         FROM df_mapped GROUP BY {cols_index}, i15 ORDER BY {cols_index}, i15)

        UNION ALL

        (SELECT {cols_index}, 35 AS agg, i, {cols_to_sum}
         FROM df_mapped GROUP BY {cols_index}, i ORDER BY {cols_index}, i)
        """
    ).df()

def ind_pattern(series, repeat=None, tile=None):
    if repeat is not None:
        series = series.repeat(repeat)
    if tile is not None:
        series = np.tile(series, tile)
    return series
