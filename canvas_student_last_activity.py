# import os
import sys
import datetime as dt
import pandas as pd
# import numpy as np
from loguru import logger
from pathlib import WindowsPath

from canvas_data.api import CanvasDataAPI


begin_datetime = '1970-01-01 00:00:00.0'

today = dt.datetime.now().strftime("%Y%m%d")

# canvas_path = WindowsPath(r"\\psc-data\E\Applications\LMS\Canvas")
# data_path = canvas_path / "Files\In\data"
# downloads_path = canvas_path / "Files\In\downloads"
# processed_path = canvas_path / "Files\CanvasData\processed_data"
canvas_path = WindowsPath(r"C:\JW\Python\Canvas")
data_path = canvas_path / "data"
downloads_path = canvas_path / "downloads"
processed_path = canvas_path / "processed_data"

# log_path = canvas_path / "Files\CanvasData\logs\canvas_student_last_activity.log"
log_path = canvas_path / "logs\canvas_student_last_activity.log"
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add(
    log_path,
    rotation="monthly",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    level="DEBUG",
)
logger.info(f"Start: Canvas - Student Last Activity")

df_user = pd.read_feather(processed_path / "user_dim.ftr")
logger.debug(f"df_user: {df_user.shape}")

df_pseudonym = pd.read_feather(processed_path / "pseudonym_dim.ftr")
logger.debug(f"df_pseudonym: {df_pseudonym.shape}")

df_terms = pd.read_feather(processed_path / "enrollment_term_dim.ftr")
logger.debug(f"df_terms: {df_terms.shape}")

df_course = pd.read_feather(processed_path / "course_dim.ftr")
logger.debug(f"df_course: {df_course.shape}")

df_sections = pd.read_feather(processed_path / "course_section_dim.ftr")
logger.debug(f"df_sections: {df_sections.shape}")

df_enrollment = pd.read_feather(processed_path / "enrollment_dim.ftr")
df_enrollment = df_enrollment.loc[(df_enrollment['workflow_state']=='active'),:]
logger.debug(f"df_enrollment: {df_enrollment.shape}")

df_u1 = df_user.merge(df_pseudonym,
              # how='left',
              left_on=['id'],
              right_on=['user_id'],
              sort=True,
             )
df_u1 = df_u1[['user_id', 'sis_user_id', 'name', 'sortable_name', 'unique_name',
     'last_request_at', 'last_login_at', 'current_login_at']    
   ]
logger.debug(f"df_u1: {df_u1.shape}")

df_s1 = df_sections.merge(df_terms,
              how='left',
              left_on=['enrollment_term_id'],
              right_on=['id'],
              suffixes=['_section', '_term']
             )
df_s1 = df_s1.loc[:,[
    'id_section', 'name_section', 'sis_source_id_section',
    'name_term', 'date_start', 'date_end', 'sis_source_id_term'
]]
logger.debug(f"df_s1: {df_s1.shape}")

df_e1 = df_enrollment.merge(df_s1,
              how='left',
              left_on=['course_section_id'],
              right_on=['id_section'],
              suffixes=['_enrollment', '_section']
             )
df_e1 = df_e1.loc[:,[
    'user_id', 'type', 'course_section_id', 'course_id', 'name_section', 'sis_source_id_section',
    'name_term', 'date_start', 'date_end', 'sis_source_id_term', 'last_activity_at',
]]
logger.debug(f"df_e1: {df_e1.shape}")

df_e2 = df_e1.merge(df_u1,
              how='left',
              left_on=['user_id'],
              right_on=['user_id'],
              suffixes=['_enrollment', '_user']
             )
df_e2 = df_e2.loc[:,[
    'user_id', 'type', 'course_section_id', 'course_id', 'name_section', 'sis_source_id_section',
    'name_term', 'date_start', 'date_end', 'sis_source_id_term',
    'sis_user_id', 'name', 'sortable_name', 'unique_name',
    'last_request_at', 'last_login_at', 'current_login_at', 'last_activity_at',
]].sort_values(['sortable_name', 'sis_source_id_section'])
df_e2 = df_e2.loc[(df_e2['type']=='StudentEnrollment'),:]
df_e2 = df_e2.loc[(df_e2['sis_source_id_term']=='2022.Fall'),:]
# df_e2['last_request_at'] = df_e2['last_request_at'].fillna(df_e2['date_start'])
# df_e2['last_login_at'] = df_e2['last_login_at'].fillna(df_e2['date_start'])
# df_e2['current_login_at'] = df_e2['current_login_at'].fillna(df_e2['date_start'])
# df_e2['last_activity_at'] = df_e2['last_activity_at'].fillna(df_e2['date_start'])
df_e2['last_request_at'] = df_e2['last_request_at'].fillna(begin_datetime)
df_e2['last_login_at'] = df_e2['last_login_at'].fillna(begin_datetime)
df_e2['current_login_at'] = df_e2['current_login_at'].fillna(begin_datetime)
df_e2['last_activity_at'] = df_e2['last_activity_at'].fillna(begin_datetime)
df_e2['login_at'] = df_e2[['current_login_at', 'last_login_at']].max(axis=1)
logger.debug(f"df_e2: {df_e2.shape}")

gb = df_e2.groupby(['sortable_name']).agg(
    {
     'sis_user_id': 'max',
     'last_activity_at': 'max',
     'last_request_at': 'max',
     'login_at': 'max',
    }
)

gb.to_csv(f"canvas_activity_{today}.csv")
logger.debug(f"Canvas - Student Last Activity written to: canvas_activity_{today}.csv")

logger.info(f"End: Canvas - Student Last Activity")
