import requests
from lxml import html
import json
import logging
import time
from datetime import datetime
from cassandra.cluster import Cluster

from cassandra_db.cassandra_settings import cass_settings

# paths to the info in the doc
X_PATHS = {
    'NAME': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/header/div/ul/li[@class="current"]/a/span/text()',
    'LAST_REPORT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[1]/div/time/text()',
    'SHIP_TYPE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[2]/span/text()',
    'FLAG': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[3]/span/text()',
    'DESTINATION': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[4]/span/text()',
    'ETA': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[5]/span/text()',
    'LATITUDE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[6]/div/span[@itemprop="latitude"]/text()',
    'LONGITUDE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[6]/div/span[@itemprop="longitude"]/text()',
    'COURSE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[7]/span/text()',
    'SPEED': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[7]/span/text()',
    'CURRENT_DRAUGHT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[8]/span[@class="small-7 columns value"]/text()',
    'CALLSIGN': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[9]/span/text()',
    'IMO': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[10]/div/span/span/text()',
    'MMSI': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/div[@class="row details-section"]/div[@id="ais-data"]/div/div[10]/div/span[2]/text()',
    'BUILT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[1]/div[1]/span[2]/text()',
    'WIDTH': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[1]/div[2]/div/span[2]/span/text()',
    'HEIGHT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[1]/div[2]/div/span[1]/span/text()',
    'DRAUGHT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[1]/div[3]/div[2]/span[1]/text()',
    'GROSS_TONAGE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[2]/div[1]/div/span[1]/text()',
    'NET_TONAGE': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[2]/div[2]/div/span[1]/text()',
    'DEAD_WEIGHT': '//body/div[1]/div[1]/div[1]/div[1]/div[1]/main/article/section[1]/div/div[@class="row details-row"]/div[2]/div[3]/div[2]/span[1]/text()'
}

logging.basicConfig(filename="spider3.log", level=logging.DEBUG)


class Spider(object):
    """Our spider class

    Used to scrap info on vessels according to MMSI
    """

    urls = list()
    responses = list()
    export = None
    vessels = list()
    sleep_delay = None

    def __init__(self, urls, sleep_delay=None, export=None):
        super(Spider, self).__init__()
        self.urls = urls
        self.export = export
        self.sleep_delay = sleep_delay

        # Set a existing conection to cassandra cluster
        cluster = Cluster(
            cass_settings['CASS_CONNECT_POINTS'],
            port=cass_settings['CASS_PORT']
        )
        self.session = cluster.connect(keyspace=cass_settings['CASS_KEYSPACE'])

    def scrap(self):
        """makes the requests, stores the responses, parse it
        returns the scrapped vessels info"""

        # requests headers to avoid 403
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
        }

        # counter
        total_url = len(self.urls)
        i = 0

        # for url in url list
        for url in self.urls:
            i += 1
            logging.info("Boat : " + str(i) + "/" + str(total_url))

            # request it
            try:
                response = requests.get(url, headers=headers)
                # logging.debug(response.content)
            except Exception as e:
                logging.error("While requesting :" + url)
                logging.error(str(e))

            # if status code isn't 200, log it
            if response.status_code != 200:
                logging.warning(
                    "Response not 200 : " + str(response.status_code)
                )
                logging.info(str(response.content))

            # keep track of the response (we never know)
            self.responses.append(response)

            # parse vessel
            vessel = self.parse(response)
            logging.debug(str(vessel))

            # if export defined, export in real time vessel parsing
            if self.export and (len(vessel) > 0):

                vessels_list = list()
                # try to load previous vessel infos
                try:
                    with open(self.export, "r") as f:
                        vessels_list = json.loads(f.read())
                except Exception as e:
                    em = "While trying to read: " + self.export + "\n" + str(e)
                    logging.info(em)

                # append new vessel to list
                vessels_list.append(vessel)

                # Adding infos in database
                cassandra_request = """INSERT INTO {}.basic_destination (mmsi, timestamps, destination) VALUES ('{}', '{}', '{}')"""
                cassandra_request = cassandra_request.format(
                    cass_settings['CASS_KEYSPACE'],
                    vessel['MMSI'],
                    str(datetime.now())[:-7],
                    vessel['DESTINATION']
                )
                try:
                    self.session.execute(cassandra_request)
                except Exception as e:
                    logging.info("Failed query : " + cassandra_request)

                # then try to write the export file
                try:
                    with open(self.export, "w") as f:
                        f.write(json.dumps(
                            vessels_list,
                            sort_keys=False,
                            indent=4,
                            separators=(",", ": ")
                        ))
                except Exception as e:
                    em = "While exporting : " + self.export + "\n" + str(vessel)
                    em += "\n" + str(e)
                    logging.error(em)

            # then sleep until next vessel
            logging.info("Spider sleeping : " + str(self.sleep_delay))
            if self.sleep_delay:
                time.sleep(self.sleep_delay)

        return self.vessels

    def parse(self, response):
        """Parsing func"""

        vessel_info = dict()

        # html as a xpath tree
        tree = html.fromstring(response.content)

        prob = False
        for key, value in X_PATHS.iteritems():
            try:
                # if successful parsing, add to the info dict
                vessel_info.update({
                    key: self.treat(key, tree.xpath(value)[0])
                })
            except Exception as e:
                # else log which attribute has been missed
                logging.warning(
                    "\nWhile trying to extract :" + str(key) + "\n" + str(e)
                )
                logging.warning(tree.xpath(value))
                # logging.debug(response.content)
                logging.info(str(response.url))
                prob = True

        if prob:
            with open("./problematic_urls.txt", "a") as f:
                #logging.debug(response.content)
                f.write(str(response.url))
                f.write("\n")

        # append to list
        self.vessels.append(vessel_info)
        return vessel_info

    def treat(self, key, string):
        """Post treatment for specific keys"""

        # clean ATD ETA ATA
        # if key in ["ATD", "ETA_ATA_check", "ETA_ATA"]:
        #     string = string.replace('\n', '')
        #     string = string.replace('\t', '')

        # if string is null then null
        if (string == 'N/A'):
            string = None
            return string

        # replace "\n" and "\t"
        string = string.replace('\n', '')
        string = string.replace('\t', '')

        if (key == 'SPEED'):
            string = string.split(
                u'\xb0')[1].replace(
                u"\xa0", '').replace(
                '/', '').replace(
                ' ', '')

        if (key == 'COURSE'):
            string = string.split(
                u'\xb0')[0].replace(
                u"\xa0", '').replace(
                '/', '').replace(
                ' ', '')
            string += 'degrees'

        logging.debug(string)

        return string

    def get_responses(self):
        return self.responses

    def get_vessels(self):
        return self.vessels


# new xpath detection
