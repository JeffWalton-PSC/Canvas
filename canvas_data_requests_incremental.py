from canvas_data.api import CanvasDataAPI
import os
import sys
import pandas as pd
from loguru import logger
from pathlib import WindowsPath

import powercampus as pc

current_yt_df = pc.current_yearterm()
current_term = current_yt_df['term'].iloc[0]
current_year = current_yt_df['year'].iloc[0]

# canvas_path = WindowsPath(r"\\psc-data\E\Applications\LMS\Canvas")
# data_path = canvas_path / "Files\In\data"
# downloads_path = canvas_path / "Files\In\downloads"
# processed_path = canvas_path / "Files\CanvasData\processed_data"
canvas_path = WindowsPath(r"C:\JW\Python\Canvas")
data_path = canvas_path / "data"
downloads_path = canvas_path / "downloads"
processed_path = canvas_path / "processed_data"
ftr_path = processed_path / "requests.ftr"
csv_path = processed_path / f"{current_term}{current_year}_requests.csv"

# log_path = canvas_path / "Files\CanvasData\logs\canvas_requests_incremental_updates.log"
log_path = canvas_path / "logs\canvas_requests_incremental_updates.log"
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add(
    log_path,
    rotation="monthly",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    level="DEBUG",
)
logger.info(f"Start: Canvas requests - incremental")

try:
    API_KEY = os.environ.get('CANVAS_DATA_API_KEY')
    API_SECRET = os.environ.get('CANVAS_DATA_API_SECRET')
except KeyError:
    logger.warning("Set CANVAS_DATA_API_KEY and CANVAS_DATA_API_SECRET environment variables.")
    exit()

cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)

schema = cd.get_schema('latest', key_on_tablenames=True)

def table_columns(table: str):
    col_names = []
    col_dtypes = {}
    col_datetimes = []
    for i in schema[table]['columns']:
        logger.debug(f"col: {i['name']}, type: {i['type']}")
        col_names.append(i['name'])
        if (i['type'] in ['varchar', 'guid', 'text', 'bigint']):
            col_dtypes[i['name']] = 'string'
        elif (i['type'] in ['boolean']):
            col_dtypes[i['name']] = 'boolean'
        elif (i['type'] in ['timestamp', 'datetime']):
            col_datetimes.append(i['name'])
        elif i['type'] in ['int', 'bigint']:
            col_dtypes[i['name']] = 'Int64'
        else:
            logger.debug( f"Add type '{i['type']}' to table_columns() function for column '{i['name']}'.")
    return col_names, col_dtypes, col_datetimes

t = 'requests'
col_names, col_dtypes, col_datetimes = table_columns(t)

fn_tsv = cd.get_data_for_table(table_name=t, 
                               dump_id='latest',
                               data_directory=data_path,
                               download_directory=downloads_path,
                              ) #, force=True)
logger.debug(f"Input file: {fn_tsv}")
df = pd.read_csv(fn_tsv, 
                sep='\t', 
                header=None, 
                names=col_names, 
                dtype=col_dtypes, 
                parse_dates=col_datetimes, 
                na_values=[r'\N']
               )
df = df.drop_duplicates()
os.remove(fn_tsv)
logger.debug(f"df:{df.shape}")

gb = ( df.groupby(['user_id', 'course_id', 'timestamp_day'])
    .agg(
        {
            'id': 'count', 
        }
    )
    .rename(
        columns={'id': 'requests'},
    )
    .reset_index()
)
logger.debug(f"gb:{gb.shape}")

# read archived requests summary
if ftr_path.exists():
    df_req = pd.read_feather(ftr_path)
    logger.debug(f"df_req:{df_req.shape}")

    # combine new requests with archived requests
    df_req = pd.concat([df_req, gb]).drop_duplicates().reset_index()

    # re-aggregate after appending new data
    gb = ( df_req[['user_id', 'course_id', 'timestamp_day', 'requests']]
            .groupby(['user_id', 'course_id', 'timestamp_day'])
            .agg(
                {
                    'requests': 'sum', 
                }
            )
            .reset_index()
    )
    logger.debug(f"gb:{gb.shape}")

gb.to_feather(ftr_path)
logger.debug(f"Data written to {ftr_path}")

gb.to_csv(csv_path, index=False)
logger.debug(f"Data written to {csv_path}")

logger.info(f"End: Canvas requests - incremental")
