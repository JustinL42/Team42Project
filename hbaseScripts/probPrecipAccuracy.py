from __future__ import print_function
import happybase as hb
from statistics import mean, variance

conn = hb.Connection()
table = conn.table("probabilityOfPrecipitation")

rainPredictions = {}
noRainPredictions = {}
for _, rowData in table.scan():
	print('.', end='')
	if 'cf:t-024h' in rowData and 'cf:t-000h' in rowData:
		code = rowData['cf:location_code']
		predictedRain = (float(rowData['cf:t-024h']) >= 50)
		actuallyRained = (float(rowData['cf:t-000h']) >= 50)
		if predictedRain:
			if code not in rainPredictions:
				rainPredictions[code] = []
			rainPredictions[code].append(actuallyRained)
		else:
			if code not in noRainPredictions:
				noRainPredictions[code] = []
			noRainPredictions[code].append(not(actuallyRained))

print("\nfinished loop")
conn.close()

rainPredictionAccuracy = [(mean(boolList), code) for code, boolList in rainPredictions.items()]
noRainPredictionAccuracy = [(mean(boolList), code) for code, boolList in noRainPredictions.items()]
rainPredictionAccuracy.sort()
noRainPredictionAccuracy.sort()


topXrain = min(5, len(rainPredictionAccuracy))
topXnorain = min(5, len(noRainPredictionAccuracy))

print("\nMost accurate predictions of rain:")
for i in range(1, topXrain + 1):
	value, code = rainPredictionAccuracy[-i]
	print("{}\t{:.3f} %".format(code, value * 100))

print("\nLeast accurate predictions of rain:")
for i in range(topXrain):
	value, code = rainPredictionAccuracy[i]
	print("{}\t{:.3f} %".format(code, value * 100))


print("\nMost accurate predictions of non-rain:")
for i in range(1, topXnorain + 1):
	value, code = noRainPredictionAccuracy[-i]
	print("{}\t{:.3f} %".format(code, value * 100))

print("\nLeast accurate predictions of non-rain:")
for i in range(topXnorain):
	value, code = noRainPredictionAccuracy[i]
	print("{}\t{:.3f} %".format(code, value * 100))

