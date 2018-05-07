import os
import weatherUtilv2
import gridpoints

# The AWS key variables need to be added to your 
# OS environment, for this to work. Or, change these 
# variables to use your AWS keys directly.
accessKey = os.environ['AWS_ACCESS_KEY_ID']
secretKey = os.environ['AWS_SECRET_ACCESS_KEY']

weatherProperties = ["probabilityOfPrecipitation", "maxTemperature", "quantitativePrecipitation"]

weatherUtilv2.retrieveDataForLocation("KAGS", weatherProperties,
									accessKey=accessKey, 
									secretKey=secretKey)
print("Got data for {}".format("KAGS"))

