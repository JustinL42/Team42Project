from __future__ import division
from __future__ import print_function
import requests
import datetime
import json
import os
import sys
from  math import floor, ceil
import isodate
import boto3 
from boto3.dynamodb.conditions import Key, Attr
# import the dictionary weather station codes in gridpoints.py
import gridpoints

# Retrieving data with these methods requires your AWS keys to be set to 'accessKey' and 'secretKey'
# in the script you are using. If they are set in your environment, you can set them like this:
# accessKey = os.environ['AWS_ACCESS_KEY_ID']
# secretKey = os.environ['AWS_SECRET_ACCESS_KEY']

# PARSERS:
	# prediction time (pTime): the time the prediction was made, according to the JSON
	# weather time (wTime): the time the prediction is about

# wProperty: Name of weather Property
# wTimeUnits: smallest time slices, in hours, for weather time periods
# pTimeUnits: smallest time slices, in hours, for hours-ahead-of-time for predictaion
# oneTimeUnitPerPrediction: if True, it is not allowed for a single prediction to cover multiple
# 							 weather time units, like it it is for probability of precipiation
def parseGeneral(weatherJSON, wProperty, wTimeUnits, pTimeUnits, oneTimeUnitPerPrediction):
	pTimeStr = weatherJSON['properties']['validTimes'].split('/')[0]
	pTime = isodate.parse_datetime(pTimeStr)
	pCanonicalHour = pTime.hour - (pTime.hour % pTimeUnits)
	pTimeCanonical = pTime.replace(hour=pCanonicalHour)

	pTimeStrCanonical = pTimeCanonical.isoformat().split('+')[0]

	for prediction in weatherJSON["properties"][wProperty]['values']:

		wTimeStr, wDurationStr = prediction['validTime'].split('/')
		wStartTime = isodate.parse_datetime(wTimeStr)
		wDuration = isodate.parse_duration(wDurationStr)
		wEndTime = wStartTime + wDuration

		wStartCanonicalHour = wStartTime.hour - (wStartTime.hour % wTimeUnits)
		wStartTimeCanonical = wStartTime.replace(hour=wStartCanonicalHour)
		wEndCanonicalHour = int(ceil(wEndTime.hour / wTimeUnits) * wTimeUnits)
		wEndTimeCanonical = wEndTime.replace(hour=0) + datetime.timedelta(hours=wEndCanonicalHour)

		currentWTimeUnit = wStartTimeCanonical
		timeUnit = datetime.timedelta(hours=wTimeUnits)
		while((wEndTimeCanonical - currentWTimeUnit).total_seconds() > 0):
			currentWTimeStr = currentWTimeUnit.isoformat().split('+')[0]
			delta = currentWTimeUnit - pTimeCanonical
			# predictions of past weather are skipped
			if delta.total_seconds() >= 0:
				# the time-ahead for predictions is standardized to 
				# the nearest number of pTimeUnits-hours times periods
				hoursAhead = int(delta.total_seconds() / 3600 / pTimeUnits) * pTimeUnits
				# the column format for predictions uses the format "t-024h" (time minus 24 hours) 
				# meaning a prediction for weather 24 hours in advance 
				column = "t-{}h".format(str(hoursAhead).zfill(3))
				if currentWTimeStr not in tables[wProperty]:
					tables[wProperty][currentWTimeStr] = {}
				value = prediction['value']
				if column not in tables[wProperty][currentWTimeStr]:
					tables[wProperty][currentWTimeStr][column] = value
				else:
					# when there are multiple predications for the same time period, use the max prediction
					tables[wProperty][currentWTimeStr][column] = max(value, tables[wProperty][currentWTimeStr][column])

			if oneTimeUnitPerPrediction:
				# stop at after recording the prediction once in this case.
				break
			currentWTimeUnit += timeUnit


def parseQuantitativePrecipitation(weatherJSON):
	wProperty = "quantitativePrecipitation"
	wTimeUnits = 6
	pTimeUnits = 6
	oneTimeUnitPerPrediction = True
	parseGeneral(weatherJSON, wProperty, wTimeUnits, pTimeUnits, oneTimeUnitPerPrediction)

def parseProbabilityOfPrecipitation(weatherJSON):
	wProperty = "probabilityOfPrecipitation"
	wTimeUnits = 12
	pTimeUnits = 6
	oneTimeUnitPerPrediction = False
	parseGeneral(weatherJSON, wProperty, wTimeUnits, pTimeUnits, oneTimeUnitPerPrediction)

def parseMaxTemperature(weatherJSON):
	wProperty = "maxTemperature"
	wTimeUnits = 24
	pTimeUnits = 24
	oneTimeUnitPerPrediction = True
	parseGeneral(weatherJSON, wProperty, wTimeUnits, pTimeUnits, oneTimeUnitPerPrediction)


# def tableToCSV(weatherProperty, filename):
def tableToCSV(wProperty, code):
	filename = wProperty + code + ".csv"
	maxHoursAheadStr = max([max(vals.keys()) for vals in tables[wProperty].values()])
	maxHoursAhead = int(maxHoursAheadStr[2:5])
	columns = ["t-{}h".format(str(t).zfill(3)) for t in range(0, maxHoursAhead + 1, 6)]
	with open(filename, 'w') as f:
		# write heading
		print("WeatherTime", end='', file=f)
		for column in columns:
				print("," + column, end='', file=f)
		print('', end="\n", file=f)

		# write data rows
		wTimes = list(tables[wProperty].keys())
		wTimes.sort()
		for wTime in wTimes:
			print(wTime, end='', file=f)
			predictions = tables[wProperty][wTime]
			for column in columns:
				value = predictions.get(column, "")
				print("," + str(value), end='', file=f)
			print('', end="\n", file=f)
		print('', end='\n', file=f)

def retrieveDataForLocation(code, listOfWeatherProperties, **kwargs): #accessKey="", secretKey=""):
	url = gridpoints.weatherStationCodeToURL[code]

	global tables
	tables = {}

	implementedWeatherParsers = set(["probabilityOfPrecipitation", "maxTemperature", "quantitativePrecipitation"])

	for wProperty in listOfWeatherProperties:
		tables[wProperty] = {}
		if wProperty in implementedWeatherParsers:
			print("Using {0} parser for property {0}.".format(wProperty))
		else:
			print("No parser implement for weather property {}.", 
				" Using the default/quantitativePrecipitation parser for this data.")

	if ('accessKey' in kwargs and 'secretKey' in kwargs):
			accessKey = kwargs['accessKey']
			secretKey = kwargs['secretKey']
	else:
		try:
			accessKey = os.environ['AWS_ACCESS_KEY_ID']
			secretKey = os.environ['AWS_SECRET_ACCESS_KEY']
		except KeyError:
			print("AWS access & secret keys must be provided ", 
				"as 2nd & 3rd arguments to retrieveDataForLocation, ", 
				"or they must be set as OS environment variables")

	for attempt in range(6):
		try:
			dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
				aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
			dynamodbTable = dynamodb.Table('WeatherJSON')
			response = dynamodbTable.query(KeyConditionExpression=Key('url').eq(url))
			while 'LastEvaluatedKey' in response:
				for item in response['Items']:
					weatherJSON = json.loads(item['data'])
					for wProperty in listOfWeatherProperties:
						if wProperty == "maxTemperature":
							parseMaxTemperature(weatherJSON)
						elif wProperty == "probabilityOfPrecipitation":
							parseProbabilityOfPrecipitation(weatherJSON)
						else:
							parseQuantitativePrecipitation(weatherJSON)

				response = dynamodbTable.query(KeyConditionExpression=Key('url').eq(url), 
												ExclusiveStartKey=response['LastEvaluatedKey'])

			# if no errors thrown, don't retry
			break
		except :
			if "ProvisionedThroughputExceededException" not in str(sys.exc_info()[0]):
				print("Unknown error while downloading {};".format(code))
				print(str(sys.exc_info()[0]))
			else:
				print("Exceeded allowed bandwidth while downloading {};".format(code))
			waitTime = 30 * (1 + attempt)
			print("waiting for {} seconds".format(waitTime))

	for wProperty in listOfWeatherProperties:
		tableToCSV(wProperty, code)
