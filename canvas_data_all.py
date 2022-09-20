from canvas_data.api import CanvasDataAPI
import os
import sys
import pandas as pd
from loguru import logger
from pathlib import WindowsPath

import powercampus as pc

# current_yt_df = pc.current_yearterm()
# current_term = current_yt_df['term'].iloc[0]
# current_year = current_yt_df['year'].iloc[0]
current_term = 'FALL'
current_year = '2022'

# canvas_path = WindowsPath(r"\\psc-data\E\Applications\LMS\Canvas")
# data_path = canvas_path / "Files\In\data"
# downloads_path = canvas_path / "Files\In\downloads"
# processed_path = canvas_path / "Files\CanvasData\processed_data"
canvas_path = WindowsPath(r"C:\JW\Python\Canvas")
data_path = canvas_path / "data"
downloads_path = canvas_path / "downloads"
processed_path = canvas_path / "processed_data"

# log_path = canvas_path / "Files\CanvasData\logs\canvas_data.log"
log_path = canvas_path / "logs\canvas_data.log"
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add(
    log_path,
    rotation="monthly",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    level="DEBUG",
)
logger.info(f"Start: Canvas Data - All tables")

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

table_list = ['user_dim', 'pseudonym_dim', 'role_dim', 'enrollment_term_dim', 'enrollment_dim', 'course_dim', 'course_section_dim']

df = {}
for t in table_list:
    fn_tsv = cd.get_data_for_table(table_name=t, 
                                   dump_id='latest', 
                                   data_directory=data_path,
                                   download_directory=downloads_path,
                                   force=True)
    logger.debug(f"{t}: {fn_tsv}")

    col_names, col_dtypes, col_datetimes = table_columns(t)
    
    df[t] = pd.read_csv(fn_tsv, 
                        sep='\t', 
                        header=None, 
                        names=col_names, 
                        dtype=col_dtypes, 
                        parse_dates=col_datetimes, 
                        na_values=[r'\N']
                       )
    logger.debug(f"df['{t}']: {df[t].shape}")

table_fields = {
    'user_dim': ['id', 'canvas_id', 'global_canvas_id', 'name', 'sortable_name'],
    'pseudonym_dim': ['id', 'canvas_id', 'user_id', 'sis_user_id', 'unique_name', 'last_request_at', 'last_login_at', 'current_login_at', ],
    'role_dim': ['id', 'canvas_id', 'name', ],
    'enrollment_term_dim': ['id', 'canvas_id', 'name', 'date_start', 'date_end', 'sis_source_id'],
    'course_dim': ['id', 'canvas_id', 'account_id', 'enrollment_term_id', 'name', 'code', 'sis_source_id'],
    'enrollment_dim': ['id', 'canvas_id', 'course_section_id', 'role_id', 'type', 'workflow_state', 'course_id', 'user_id', 'last_activity_at'],
    'course_section_dim': ['id', 'canvas_id', 'course_id', 'enrollment_term_id', 'name', 'sis_source_id'],
}

for t in table_list:
    df_t = df[t].loc[:,table_fields[t]]
    ftr_path = processed_path / f"{t}.ftr"
    if df_t.empty:
        logger.debug(f"{t}:{df_t.shape} is empty.")
    else:
        df_t.reset_index(drop=True).to_feather(ftr_path)
        logger.debug(f"{t}:{df_t.shape} data written to {ftr_path}")

logger.info(f"End: Canvas Data - All tables")
