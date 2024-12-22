import os
import json
import logging
import time
from kafka import KafkaProducer
import urllib.request
from dotenv import load_dotenv



logging.basicConfig(level=logging.INFO)

#load the api key from env variables
def configure():
    load_dotenv()

# kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# main function of the producer
def main():

    #url with api key for requesting the stations data
    url = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={os.getenv('API_KEY')}"
    # requesting data and send to kafka broker
    while True:
        response = urllib.request.urlopen(url)
        stations_data = json.loads(response.read().decode())
        for station in stations_data:
            station_data = {
                'number': station['number'],
                'contract_name': station['contract_name'],
                'station_name': station['name'],
                'address': station['address'],
                'status': station['status'],
                'lattitude': station['position']['lat'],
                'longitude': station['position']['lng'],
                'total_bike_stands': station['bike_stands'],
                'available_bike_stands': station['available_bike_stands'],
                'available_bikes': station['available_bikes'],
                'last_update': station['last_update']
            }
            # Send formatted data to Kafka topic
            producer.send("stations", station_data)
        logging.info("{} Produced {} station records".format(time.time(), len(stations_data)))
        time.sleep(60)

if __name__=="__main__":
    configure()
    main()