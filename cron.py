import json
import logging
from datetime import date, datetime, timedelta
from mmsi.spider import Spider


# >>> Logger
logging.basicConfig(filename="cron.log", level=logging.INFO)

# >>> Export paths and filenames settings
EXPORT_MMSI_FOLDER_PATH = "./mmsi_lists/"
EXPORT_MMSI_FILENAME = "MMSI_list.json"
EXPORT_ADD_INFO_FOLDER_PATH = "./mmsi_info/"
EXPORT_ADD_INFO_FILENAME = "MMSI_info.json"

# >>> Yesterday
yesterday = date.today() - timedelta(1)
yesterday = yesterday.strftime('%Y_%m_%d-')

# >>> load MMSI list :
filename = EXPORT_MMSI_FOLDER_PATH + yesterday + EXPORT_MMSI_FILENAME
with open(filename, "r") as f:
    mmsi_list = json.loads(f.read())
logging.info(str(mmsi_list))

# >>> Everyday from 00:01, for 22hours ?
url = 'https://www.vesselfinder.com/vessels/PRDW-MMSI-'
mmsi_list = [url + x for x in mmsi_list]
logging.info(str(mmsi_list))

# >>> Scraping
# we don't want to make too much calls at the same time, so we scrap over a time
# period of 22hours (arbitrary).
# 22 hours x 60 minutes = 1 320 minutes
# Sleep delay = 60 seconds / (Boat per minute)
# with boat per minute = total MMSI / 22hours (in minute)
sleep_delay = int(60. / (float(len(mmsi_list)) / 1320.))
logging.info("SLEEP DELAY :" + str(sleep_delay))

export_filename = EXPORT_ADD_INFO_FOLDER_PATH + yesterday
export_filename += EXPORT_ADD_INFO_FILENAME
spider = Spider(
    mmsi_list,
    sleep_delay=sleep_delay,
    export=export_filename
)
spider.scrap()
