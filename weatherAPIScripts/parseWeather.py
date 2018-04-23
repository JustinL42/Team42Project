from __future__ import division
from __future__ import print_function
import requests
import datetime
import json
import os
from  math import floor
import dateutil.parser
import boto3 
from boto3.dynamodb.conditions import Key, Attr
# import the dictionary weather station codes in gridpoints.py
import gridpoints

# The AWS key variables need to be added to your OS environment, for this to work.
# Or, change these variables to use your AWS keys directly.
accessKey = os.environ['AWS_ACCESS_KEY_ID']
secretKey = os.environ['AWS_SECRET_ACCESS_KEY']

# weatherProperty is probability of precipitation, max temperature, or other weather feature from the JSON
def parse(weatherJSON, weatherProperty, table):
	# prediction time: the time the prediction was made, according to the JSON
	# weather time: the time the prediction is about
	# all times are rounded to the nearest quarter day, 6-hour period
	predictionTimeStr = weatherJSON['properties']['validTimes'].split('/')[0]
	predictionTime = dateutil.parser.parse(predictionTimeStr)
	predictionCanonicalHour = int(floor(predictionTime.hour / 6) * 6)
	predictionTimeCanonical = predictionTime.replace(hour=predictionCanonicalHour)
	predictionTimeStrCanonical = predictionTimeCanonical.isoformat().split('+')[0]

	for prediction in weatherJSON["properties"][weatherProperty]['values']:
		weatherTimeStr = prediction['validTime'].split('/')[0]
		weatherTime = dateutil.parser.parse(weatherTimeStr)
		weatherCanonicalHour = int(floor(weatherTime.hour / 6) * 6)
		weatherTimeCanonical = weatherTime.replace(hour=weatherCanonicalHour)
		weatherTimeStrCanonical = weatherTimeCanonical.isoformat().split('+')[0]
		delta = weatherTimeCanonical - predictionTimeCanonical
		# predictions of past weather are skipped
		if delta.total_seconds() >= 0:
			# the time-ahead for predictions is standardized to the nearest number of 6-hours times periods
			hoursAhead = int(delta.total_seconds() / 3600 / 6) * 6
			# the column format for predictions uses the format "t-024h" (time minus 24 hours) 
			# meaning a prediction for weather 24 hours in advance 
			column = "t-{}h".format(str(hoursAhead).zfill(3))
			if weatherTimeStrCanonical not in table:
				table[weatherTimeStrCanonical] = {}
			value = prediction['value']
			if column not in table[weatherTimeStrCanonical]:
				table[weatherTimeStrCanonical][column] = value
			else:
				# when there are multiple predications for the same 6-hour period, use the max prediction
				table[weatherTimeStrCanonical][column] = max(value, table[weatherTimeStrCanonical][column])


def tableToCSV(table, filename):
	maxHoursAheadStr = max([max(vals.keys()) for vals in table.values()])
	maxHoursAhead = int(maxHoursAheadStr[2:5])
	columns = ["t-{}h".format(str(t).zfill(3)) for t in range(0, maxHoursAhead + 1, 6)]
	with open(filename, 'w') as f:
		# write heading
		print("WeatherTime", end='', file=f)
		for column in columns:
				print("," + column, end='', file=f)
		print('', end="\n", file=f)

		# write data rows
		weatherTimes = table.keys()
		weatherTimes.sort()
		for weatherTime in weatherTimes:
			print(weatherTime, end='', file=f)
			predictions = table[weatherTime]
			for column in columns:
				value = predictions.get(column, "")
				print("," + str(value), end='', file=f)
			print('', end="\n", file=f)
		print('', end='\n', file=f)

def retrieveDataForLocation(code):
	url = gridpoints.weatherStationCodeToURL[code]
	precipitationTable = {}
	maxTempTable = {}

	dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
	    aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
	dynamodbTable = dynamodb.Table('WeatherJSON')
	response = dynamodbTable.query(KeyConditionExpression=Key('url').eq(url))
	while 'LastEvaluatedKey' in response:
		for item in response['Items']:
			weatherJSON = json.loads(item['data'])
			parse(weatherJSON, "probabilityOfPrecipitation", precipitationTable)
			parse(weatherJSON, "maxTemperature", maxTempTable)
		response = dynamodbTable.query(KeyConditionExpression=Key('url').eq(url), 
										ExclusiveStartKey=response['LastEvaluatedKey'])

	tableToCSV(precipitationTable, "probababilityOfPrecipitation" + code + ".csv")
	tableToCSV(maxTempTable, "maxTemperature" + code + ".csv")

# weather station nearest UIUC:
# "stationIdentifier": "KCMI",
# "name": "University of Illinois - Willard",
# "timeZone": "America/Chicago",
retrieveDataForLocation("KCMI")

# weather Station nearest Augusta, Georgia
# "stationIdentifier": "KDNL",
# "name": "Augusta Daniel Field",
# "timeZone": "America/New_York",
retrieveDataForLocation("KDNL")

