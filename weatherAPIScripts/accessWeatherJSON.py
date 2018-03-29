import time
import requests
import boto3
import datetime
import json
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
# note: the AWS key variables need to be added to your OS environment, for this to work.
# Or, change the code to use your AWS keys directly.

table = dynamodb.Table('WeatherJSON')

# Here is an example of how to access the unprocessed forecast data for a specific location
# The closest weather station to Urbana, IL is:
# id": "https://api.weather.gov/stations/KCMI",
# coordinates": [ -88.27547, 40.03245 ]
# stationIdentifier": "KCMI",
# name": "University of Illinois - Willard",
# timeZone": "America/Chicago"
# Look at the current forecast from this station at:
# https://api.weather.gov/gridpoints/ILX/94,68



def downloadAll(gridPoints):
	print("Starting to download gridpoints")
	for locID, url in gridPoints.items():
		retries = 7
		while retries > 0:
			retries -= 1
			try: 
				r = requests.get(url)
				# check that the JSON contains precipitation data. If not, try again.
				prec = r.json()["properties"]["probabilityOfPrecipitation"]["values"][0]["value"]
				retries = 0
				# debugging:
				# print "{}: {}%".format(locID, prec)
				table.put_item(Item={'url': url, 
									 'timestamp': datetime.datetime.now().isoformat(), 
									 'data': json.dumps(r.json())
							  })
			except KeyError:
				# wait a few seconds before trying again
				time.sleep(3)
	print("Finished downloading gridpoints")
