import time
import requests
import boto3 
from boto3.dynamodb.conditions import Key, Attr

import datetime
import json
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
# note: the AWS key variables need to be added to your OS environment, for this to work.
# Or, change the code to use your AWS keys directly.

table = dynamodb.Table('WeatherJSON')

print("\nTotal count of items in the table")
print(table.item_count)


# Here is an example of how to access the unprocessed forecast data for a specific location
# The closest weather station to Urbana, IL is:
# id": "https://api.weather.gov/stations/KCMI",
# coordinates": [ -88.27547, 40.03245 ]
# stationIdentifier": "KCMI",
# name": "University of Illinois - Willard",
# timeZone": "America/Chicago"
# Look at the current forecast from this station at:
# https://api.weather.gov/gridpoints/ILX/94,68


uiucUrl = "https://api.weather.gov/gridpoints/ILX/94,68"

response = table.query(
    KeyConditionExpression=Key('url').eq(uiucUrl),
    # get just two items for now
    Limit=2
)

# get item from the query results
for item in response['Items']:
	# convert the json in string format to a json object
	weatherJSON = json.loads(item['data'])

	print("\nTime of predictions:")
	print(weatherJSON["properties"]['updateTime'])

	print("\npredicted time:\t\t\t\tprobability of precipitation")
	for predication in weatherJSON["properties"]["probabilityOfPrecipitation"]["values"]:
		print('{}\t\t{}%'.format(predication['validTime'],predication['value']))
