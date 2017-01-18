import requests
import json
import time
import logging

from datetime import datetime

from cassandra.cluster import Cluster

from cassandra_db.cassandra_settings import cass_settings

# >>> WE LOG EVERYTHING IN THAT FILE
logging.basicConfig(filename="get_data.log", level=logging.WARNING)

# >>> TIME INTERVAL BETWEEN TWO CALLS SETTING
TIME_INTERVAL = 60

# >>> Filenames setting
EXPORT_FILENAME = "AIS_dump.json"
EXPORT_MMSI_FILENAME = "MMSI_list.json"

# >>> Export folders settings
EXPORT_FOLDER_PATH = "./dumps2/"
EXPORT_MMSI_FOLDER_PATH = "./mmsi_lists/"

# >>> Checking our script works well
LOG_THIS_VESSEL = ''

# >>> REQUEST HEADERS (from chrome console)
# GET /vesselsonmap?bbox=-3.9111328124999556%2C25.826945071199916%2C36.69433593750004%2C47.803824046510385&zoom=5&mmsi=0&show_names=0&ref=85988.88209481971&pv=6 HTTP/1.1
# Host: www.vesselfinder.com
# Connection: keep-alive
# Accept: */*
# X-Requested-With: XMLHttpRequest
# User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36
# DNT: 1
# Referer: https://www.vesselfinder.com/
# Accept-Encoding: gzip, deflate, sdch, br
# Accept-Language: fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4
# Cookie: PHPSESSID=32e6c9d0e4dd93dc54396f4d9b9f78cc; mapType=full; _gat=1; tf=0; jrj=1; mapFilter2=511; _ga=GA1.2.38061665.1478270811; ls=; shipNames=auto; myships=0; vesselfinder-mapPosition=5,16.391601562500046,37.620631600464506

# >>> Coordinates
# TODO : determinate correspondance between this and reality
# Coordinates in DD normalisation
COORD_1 = "-4,"  # BOTTOM_LEFT_EAST
COORD_2 = "26,"  # BOTTOM_LEFT_NORTH
COORD_3 = "37,"  # TOP_RIGHT_EAST
COORD_4 = "48"  # TOP_RIGHT_NORTH
ZOOM_LVL = "&zoom=8"  # 8 seems a nice value to begin with

# >>> REQUESTS HEADERS NOT TO GET 403
HEADERS = {'X-Requested-With': 'XMLHttpRequest'}
# could alternate between different headers to avoid getting banned /!\

# Set a existing conection to cassandra cluster
cluster = Cluster(cass_settings['CASS_CONNECT_POINTS'], port=cass_settings['CASS_PORT'])
session = cluster.connect(keyspace=cass_settings['CASS_KEYSPACE'])

while (True):

    # >>> USED FOR EXPORT FILENAMES
    now = datetime.now().strftime('%Y_%m_%d-%H_')
    today = datetime.now().strftime('%Y_%m_%d-')

    # >>> URL COMPOSITION
    url = "https://www.vesselfinder.com/vesselsonmap?bbox="
    # Four coordinate variables
    url += COORD_1 + COORD_2 + COORD_3 + COORD_4
    url += ZOOM_LVL
    url += "&mmsi=0&show_names=0&ref=66806.32884773801&pv=6"
    logging.info(url)

    try:
        # >>> Request
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            err_message = str(datetime.now()) + " Errored Response status : "
            err_message += str(response)
            logging.error(err_message)

        # >>> Cleaning the raw response
        d_list = response.content.split("\n")
        d_list = [x.split("\t") for x in d_list]

        # >>> loading previous
        # FILENAME CONVENTION for data export (arbitrary) :
        # Example : ./dumps2/2016_11_21_16_AIS_dump.json
        # For the 21/11/2016 at 16:00
        filename = EXPORT_FOLDER_PATH + now
        filename += EXPORT_FILENAME

        cleand = list()
        try:
            with open(filename, "r") as f:
                cleand = json.loads(f.read())
        except Exception as e:
            logging.info("While trying to read : " + filename + ":\n" + str(e))

        mmsi_list = list()

        # >>> For line in the cleaned response
        for l in d_list:
            try:  # some lines are empty
                d = {
                    'init_LAT': l[0],  # = LAT * 600
                    'init_LONG': l[1],  # = LONG * 600
                    'COG': l[2],
                    'SOG': l[3],
                    'HEADING': l[4],
                    'MMSI': l[5],
                    'PAC': l[6],
                    'timestamp': str(datetime.now())
                }
                d.update({
                    'LAT': str(float(d['init_LAT']) / 600000.),
                    'LONG': str(float(d['init_LONG']) / 600000.),
                })

                # Copy of Dictionary for cassandra insertion
                c = d.copy()

		# Treament on datas to prepare insertion in cass
                del c['init_LAT']
                del c['init_LONG']
                c['course_over_ground'] = int(c['COG'])/10
                del c['COG']
                c['speed_over_ground'] = int(c['SOG'])
                del c['SOG']
                c['heading'] = int(c['HEADING'])
                del c['HEADING']
                c['timestamps'] = c['timestamp'][:-7]
                del c['timestamp']
                c['latitude'] = c['LAT']
                del c['LAT']
                c['longitude'] = c['LONG']
                del c['LONG']
                line = """INSERT INTO basic_position JSON '{}';""".format(json.dumps(c))
                print line
                try:
                    session.execute(line)
                except Exception:
                    print "Problem"
                print d

                # if we want to track a vessel by MMSI for debug
                if d['MMSI'] == LOG_THIS_VESSEL:
                    log_mess = str(d)
                    logging.warning(log_mess)

                cleand.append(d)
                mmsi_list.append(d['MMSI'])
            except Exception as e:
                if len(l) > 1:
                    logging.error(str(e) + " --> with l : " + str(l))

        logging.info("CLEAND SIZE :" + str(len(cleand)))  # shouldnt be empty
        logging.info("MMSI LIST SIZE :" + str(len(mmsi_list)))  # idem

        with open(filename, "w") as f:
            f.write(json.dumps(
                cleand,
                sort_keys=False,
                indent=4,
                separators=(",", ": ")
            ))

        # UPDATING THE MMSI_LIST
        # Convention (arbitrary) :
        filename = EXPORT_MMSI_FOLDER_PATH + today
        filename += EXPORT_MMSI_FILENAME
        mmsi = list()

        # read previous mmsi list
        try:
            with open(filename, "r") as f:
                mmsi = json.loads(f.read())
        except Exception as e:
            logging.info("While trying to read : " + filename + ":\n" + str(e))

        # extend it with the new ones
        mmsi.extend(x for x in mmsi_list if x not in mmsi)

        # export the result
        try:
            with open(filename, "w") as f:
                f.write(json.dumps(mmsi))
        except Exception as e:
            err_mess = "While trying write in : " + filename + ":\n" + str(e)
            logging.warning(err_mess)

    except Exception as e:
        logging.error(str(datetime.now()) + " while requesting -> " + url)
        logging.error(str(e))

    time.sleep(TIME_INTERVAL)

exit()
