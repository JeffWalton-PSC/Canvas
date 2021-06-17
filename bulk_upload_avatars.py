#!/usr/bin/python

# Based on this script: 
# https://github.com/unsupported/canvas/blob/master/api/bulk_assign_avatars/python/bulk_upload_avatars.py

import csv, requests, os, sys
import mimetypes

from loguru import logger
from pathlib import Path

## TO DO:  Add CLI interface for logger debug mode, input csv file, canvas_host identifier

working_path = Path(r"./")
csv_filename = "avatar_upload.csv"
images_url = "https://selfservice.paulsmiths.edu/SelfService/PeopleImages/"
# images_path = Path(r"\\psc-data\E\Applications\Starfish\Files\prod\sisdatafiles\studentFiles\studentPhotos")  
log_filename = "upload_avatars_log.txt"
canvas_host = ""  # valid hosts are: production="", test="-TEST", beta="-BETA" 

# Canvas API URL
API_URL = os.environ.get(f"CANVAS{canvas_host}_API_URL")
if not API_URL:
    raise KeyError("CANVAS_API_URL is not set.")
#print(f"API_URL: {API_URL}")

# Canvas API key
API_TOKEN = os.environ.get(f"CANVAS{canvas_host}_API_TOKEN")
if not API_TOKEN:
    raise KeyError("CANVAS_API_TOKEN is not set.")
#print(f"API_TOKEN: {API_TOKEN}")


##############################################################################
##############################################################################


logger.remove()
logger.add(
    f"{working_path/ 'logs' / log_filename}",
    rotation="monthly",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name} | {message}",
    level="INFO"
)
logger.add(sys.stderr, level="INFO")
logger.info(f"\n\nStart")
logger.info(f"API_URL: {API_URL}")

header = {'Authorization' : f'Bearer {API_TOKEN}'}
logger.debug(header)

csv_file_reader = csv.DictReader(open(working_path / csv_filename, 'r'))

valid_mimetypes = ('image/jpeg','image/png','image/gif')


for user in csv_file_reader:
    file_id = None
    user_sis_id = user['sis_id']
    inform_api_url = f"{API_URL}/api/v1/users/self/files"
    image_filename = f"{user_sis_id}.jpg"
    image_url = f"{images_url}{image_filename}"
    logger.debug(f"image_url: {image_url}")

    # Step 0: Check for existing avatar
    user_info_url = f"{API_URL}/api/v1/users/sis_user_id:{user_sis_id}"
    logger.debug(f"user_info_url: {user_info_url}")
    r = requests.get(user_info_url, headers=header)
    if r.status_code != 200:
        logger.info(f"User: {user_sis_id} not found in Canvas.  Skipping...")
        continue
    json_r = r.json()
    user_canvas_id = json_r['id']
    user_name = json_r['name']
    current_avatar_url = json_r['avatar_url']
    logger.info(f"User: {user_canvas_id} {user_name}, SIS: {user_sis_id}, current_avatar_url: {current_avatar_url}")
    if ("dotted_pic.png" not in current_avatar_url) and ("avatar-50.png" not in current_avatar_url) and (".beta." not in current_avatar_url):
        logger.info(f"User: {user_sis_id} Avatar exists.  Skipping...")
        continue

    # Step 0.1: Check image exists
    if not requests.get(image_url):
        logger.info(f"User: {user_sis_id} Image NOT FOUND.  Skipping...  {image_url}")
        continue    

    # Step 1: Start upload file to user's file storage in Canvas
    mime_type,encoding = mimetypes.guess_type(image_url)
    logger.debug(f"mime_type: {mime_type}, encoding: {encoding}")
    if not mime_type in valid_mimetypes:
        logger.error(f'Not a valid mimetype: {mime_type}')
        continue
    inform_parameters = {
        'url':image_url,
        'name':image_filename,
        #'size':os.path.getsize(image_path), # read the filesize
        'content_type':mime_type,
        'parent_folder_path':'profile pictures',
        'as_user_id': f"sis_user_id:{user_sis_id}"
        }
    logger.debug(inform_parameters)
    res = requests.post(inform_api_url, headers=header, data=inform_parameters)
    if not res:
        logger.error(f"{user_sis_id} Inform request failed. {res}\nres.json: {res.json()}")
    logger.debug(f"res.url: {res.url}")
    logger.debug(f"res.text: {res.text}")
    logger.debug(f"res.json: {res.json()}")
    
    logger.info("Done prepping Canvas for upload, now sending the data...")
    
    json_res = res.json()
    logger.debug(f"json_res: {json_res}")

    # Step 2:  Upload data
    logger.debug("json_res['upload_params']",json_res['upload_params'])
    logger.debug("json_res['upload_url']",json_res['upload_url'])


    logger.info("Yes! Done sending pre-emptive 'here comes data' data, now uploading the file...")
    upload_file_response = requests.post(json_res['upload_url'],data=json_res['upload_params'],allow_redirects=False)
    if not upload_file_response:
        logger.error(f"{user_sis_id} Upload failed. {res}\nres.json: {upload_file_response.json()}")

    # Step 3: Confirm upload
    logger.info("Done uploading the file, now confirming the upload...")
    confirmation = requests.post(upload_file_response.headers['location'],headers=header)

    if 'id' in confirmation.json():
        file_id = confirmation.json()['id']
        params = { 'as_user_id': f"sis_user_id:{user_sis_id}" }
        file_info = requests.get(f"{API_URL}/api/v1/files/{file_id}",headers=header,params=params)
        logger.debug(f"{file_info.json()}")
    else:
        logger.error(f"no file_id\n{confirmation.json()}")
        continue

    logger.debug(f"file_id: {file_id}")
    logger.info("upload confirmed... nicely done!")

    # Step 4: Make api call to set avatar image to the token of the uploaded imaged (file_id)
    params = { 'as_user_id': f"sis_user_id:{user_sis_id}" }
    avatar_options = requests.get(f"{API_URL}/api/v1/users/{user_canvas_id}/avatars",headers=header,params=params)

    for ao in avatar_options.json():
        logger.debug(ao.keys())
        if ao.get('display_name') == image_filename:
            logger.info(f"{ao.get('display_name')} avatar option found...")
            logger.debug((ao.get('display_name'), ao.get('token'), ao.get('url')))
            params['user[avatar][token]'] = ao.get('token')

            set_avatar_user = requests.put(f"{API_URL}/api/v1/users/{user_canvas_id}",headers=header,params=params)
            if set_avatar_user.status_code == 200:
                logger.info(f"success uploading user avatar for {user_canvas_id} {user_sis_id}")
                logger.debug(f"set_avatar_user:\n{set_avatar_user}")


logger.info("Done.")
