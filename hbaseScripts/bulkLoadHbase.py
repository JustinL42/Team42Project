import os
import weatherUtilHBase
import gridpoints
import happybase as hb

conn = hb.Connection()

# The AWS key variables need to be added to your 
# OS environment, for this to work. Or, change these 
# variables to use your AWS keys directly.
accessKey = os.environ['AWS_ACCESS_KEY_ID']
secretKey = os.environ['AWS_SECRET_ACCESS_KEY']

weatherProperties = ["probabilityOfPrecipitation", "maxTemperature", "quantitativePrecipitation"]
for code in gridpoints.weatherStationCodeToURL.keys():
	weatherUtilHBase.retrieveDataForLocation(code, weatherProperties,
										accessKey=accessKey, 
										secretKey=secretKey,
										connection=conn)
	print("Got data for {}".format(code))

