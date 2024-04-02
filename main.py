#!/var/local/s3/venv/bin/python3

import datetime
import os, sys
from pathlib import Path
import pytz

import boto3
from boto3.s3.transfer import S3Transfer

from loguru import logger

import threading

logger.add("/var/local/s3/s3_log.log")

S3_BUCKET_NAME = "farm-backups"
S3_DIRNAME = "vm"
TIME_ZONE = os.getenv("TIME_ZONE", "Asia/Tomsk")

BACKUP_DIRECTORY = "/zPool/zDir/dump"
BACKUP_ENC_DIRECTORY = "/zPool/zDir/dump/enc"
BACKUP_FILENAME_LIST = os.listdir(BACKUP_DIRECTORY)

def get_current_date_str():
    now = datetime.datetime.now(pytz.timezone(TIME_ZONE))
    return now.strftime('%Y_%m_%d')

def get_s3_instance():
    session = boto3.session.Session()
    return session.client(service_name='s3', endpoint_url='https://storage.yandexcloud.net')

def upload_dump_to_s3(upl_filename: str, backup_date: str):
    logger.info("Starting upload to Object Storage")
    #print(f'{S3_DIRNAME}/{get_current_date_str()}/{upl_filename}')
    get_s3_instance().upload_file(Filename=f'{BACKUP_ENC_DIRECTORY}/{upl_filename}', Bucket=S3_BUCKET_NAME, Key=f'{S3_DIRNAME}/{backup_date}/{upl_filename}')
    logger.info("Uploaded")

def get_last_backup_filename():
    s3 = get_s3_instance()
    dumps = s3.list_objects(Bucket=S3_BUCKET_NAME)['Contents']
    print(dumps)
   # dumps.sort(key=lambda x: x['LastModified'])
   # last_backup_filename = dumps[-1]
   # print(f"\U000023F3 Last backup in S3 is {last_backup_filename['Key']}, "
   # f"{round(last_backup_filename['Size'] / (1024*1024))} MB, "
   # f"download it")
   # return last_backup_filename['Key']
#### TEST #####

def upload(file, input):
    transfer = S3Transfer(s3)
    size = float(os.path.getsize(file))
    transfer.upload_file(**input, callback=_progress(file, size, 'Upload'))

def _progress(filename, size, ops):
    """ indicator to calculate progress based on filesize"""
    _filename = filename
    _size = size
    _seen_so_far = 0
    _ops = ops
    _lock = threading.Lock()

    def call(bytes_amount):
        with _lock:
            nonlocal _seen_so_far
            _seen_so_far += bytes_amount
            percentage = (_seen_so_far / _size) * 100
            logging.info("%s: %s  %s / %s  (%.2f%%)" % (_ops, _filename, _seen_so_far, _size,percentage))
    return call


#### end test ####

if __name__ == "__main__":
    try:
        arg = sys.argv[1]
        if arg and arg == "job-end":

            logger.info(f"START PROGRAM - {datetime.datetime.now(pytz.timezone(TIME_ZONE))}")
            curent_date = get_current_date_str()
            for FileName in BACKUP_FILENAME_LIST:
                if str(curent_date) in FileName and not FileName.endwith(".log"):
                    logger.info(f"START WORK {FileName} - {datetime.datetime.now(pytz.timezone(TIME_ZONE))}")
                    #print(f"""{BACKUP_DIRECTORY + FileName}""")
                    with open((f"""{BACKUP_DIRECTORY}/{FileName}"""), "rb") as file:
                        operation_status = os.WEXITSTATUS(os.system(f"""gpg --batch --yes -e -a -r FarmBackup@s3.ru -o {BACKUP_ENC_DIRECTORY}/{FileName}.enc   {BACKUP_DIRECTORY}/{FileName}"""))

                        file.close()
                    logger.info(f"END WORK {FileName} - {datetime.datetime.now(pytz.timezone(TIME_ZONE))}")

            for FileName in os.listdir(BACKUP_ENC_DIRECTORY):
                try:
                    upload_dump_to_s3(FileName, curent_date)
                except Exception as e:
                    logger.error(e)
                else:
                    logger.success(f"remove {BACKUP_ENC_DIRECTORY}/{FileName}")
                    os.system(f"""rm {BACKUP_ENC_DIRECTORY}/{FileName}""")

            logger.info(f"END PROGRAM - {datetime.datetime.now(pytz.timezone(TIME_ZONE))}")

    except IndexError as e:
        logger.ettor(e)
