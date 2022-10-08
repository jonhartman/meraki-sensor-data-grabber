import time
import meraki
import logging
import os
import sys
from dateutil import parser
from influxdb import InfluxDBClient


DEBUGGING = False
DB_HOST = '10.10.10.21'
DB_NAME = 'metrics'
PID_FILE = "./.pid"
LOG_FILE = "./logfile"


def grab_sensor_names():
    # Create mapping of serials to friendly names
    translationTable = {}

    try:
        inventory = dashboard.organizations.getOrganizationInventoryDevices(410499, productTypes="sensor")
    except Exception as error:
        logging.error("UNABLE TO PULL INVENTORY:\t" + str(error))
    else:
        for inventoryItem in inventory:
            translationTable[inventoryItem['serial']] = inventoryItem['name']

    return translationTable


def process_sensor_data(retrievedData):
    output = []

    for reading in retrievedData:

        readingEntry = {
            'net': reading['network']['id'],
            'sn': reading['serial'],
            'name': sensorNames[reading['serial']],
            'metric': reading['metric']
        }

        if DEBUGGING:
            readingEntry['ts'] = reading['ts']
        else:
            readingEntry['ts'] = str(int(parser.parse(reading['ts']).timestamp()))

        if reading['metric'] == 'door':
            if reading['door']['open']:
                readingEntry['value'] = 1
            else:
                readingEntry['value'] = 0

        elif reading['metric'] == 'temperature':
            readingEntry['value'] = reading[reading['metric']]['fahrenheit']

        elif reading['metric'] == 'humidity':
            readingEntry['value'] = reading[reading['metric']]['relativePercentage']

        elif reading['metric'] == 'tvoc':
            readingEntry['value'] = reading[reading['metric']]['concentration']

        elif reading['metric'] == 'eco2':
            readingEntry['value'] = reading[reading['metric']]['concentration']

        elif reading['metric'] == 'pm25':
            readingEntry['value'] = reading[reading['metric']]['concentration']

        elif reading['metric'] == 'noise':
            readingEntry['value'] = reading[reading['metric']]['ambient']['level']

        elif reading['metric'] == 'water':
            if reading[reading['metric']]['present']:
                readingEntry['value'] = 1
            else:
                readingEntry['value'] = 0

        elif reading['metric'] == 'indoorAirQuality':
            readingEntry['value'] = reading[reading['metric']]['score']

        elif reading['metric'] == 'button':
            if reading[reading['metric']]['pressType'] == 'short':
                readingEntry['value'] = 1
            elif reading[reading['metric']]['pressType'] == 'long':
                readingEntry['value'] = 2
            else:
                readingEntry['value'] = 0

        elif reading['metric'] == 'battery':
            readingEntry['value'] = reading[reading['metric']]['percentage']

        else:
            logging.error("Unknown data type: " + reading)

        if " " in readingEntry['name']:
            escapedName = readingEntry['name'].replace(' ', '\ ')
        else:
            escapedName = readingEntry['name']

        # generate output formatted in line protocol format
        output.append(
            f"{readingEntry['metric']},"
            f"network={readingEntry['net']},serial={readingEntry['sn']},sensorName={escapedName} "
            f"{readingEntry['metric']}={readingEntry['value']} "
            f"{readingEntry['ts']}")

    strOutput = '\n'.join(output)
    return strOutput


def write_data(datapoints):

    if DEBUGGING:
        print(datapoints)
    else:

        # Initialize DB
        if "DB_USER" not in globals():
            DB_USER = os.getenv('DB_USER')
        if "DB_PASS" not in globals():
            DB_PASS = os.getenv('DB_PASS')

        # Connect, write data, and close connection
        try:
            db = InfluxDBClient(host=DB_HOST,
                                port=8086,
                                username=DB_USER,
                                password=DB_PASS,
                                ssl=False,
                                verify_ssl=False
                                )
        except Exception as dbConnectError:
            logging.critical(f"UNABLE TO CONNECT TO DB:\t {dbConnectError}")
        else:
            try:
                db.switch_database(DB_NAME)
            except Exception as dbSelectError:
                logging.critical(f"UNABLE TO SELECT DB:\t {dbSelectError}")
            else:
                try:
                    db.write_points(datapoints, time_precision='s', protocol='line')
                except Exception as dbWriteError:
                    logging.critical(f"UNABLE TO WRITE TO DB:\t {dbWriteError}")
                else:
                    try:
                        db.close()
                    except Exception as dbCloseError:
                        logging.critical(f"UNABLE TO CLOSE DB:\t {dbCloseError}")


logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.INFO)
logging.critical('Script started')

dashboard = meraki.DashboardAPI(suppress_logging=False, single_request_timeout=7)

if not DEBUGGING:
    # Fork a child process and exit
    pid = os.fork()
    pidFile = open(PID_FILE, "w")
    pidFile.writelines(str(os.getpid()))
    pidFile.close()

    if pid > 0:
        sys.exit(1)


while True:

    # Populate the S/N-to-name table
    sensorNames = grab_sensor_names()

    # Grab the sensor readings
    rawSensorData = dashboard.sensor.getOrganizationSensorReadingsHistory(410499, total_pages=-1, timespan=1800)

    # Turn the readings to usable data
    processedData = process_sensor_data(rawSensorData)

    # Write it out
    write_data(processedData)

    time.sleep(10)
