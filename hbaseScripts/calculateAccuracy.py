from __future__ import print_function
import happybase as hb
from statistics import mean, variance

conn = hb.Connection()
table = conn.table("maxTemperature")

predictionDiff = {}
for _, rowData in table.scan(limit=50):
	print('.', end='')
	if 'cf:t-024h' in rowData and 'cf:t-000h' in rowData:
		code = rowData['cf:location_code']
		predicted = float(rowData['cf:t-024h'])
		actual = float(rowData['cf:t-000h'])
		if code not in predictionDiff:
			predictionDiff[code] = []
		predictionDiff[code].append(predicted - actual)

print("\nfinished loop")
conn.close()

averageDifference = [(abs(mean(diffList)), mean(diffList), code) for code, diffList in predictionDiff.items() ]
differenceVariance = [(variance(diffList), code) for code, diffList in predictionDiff.items() ]
averageDifference.sort()
differenceVariance.sort()

topX = min(5, len(averageDifference))

print("\nMost accurate high temp. predictions:")
for i in range(topX):
	absValue, value, code = averageDifference[i]
	print("{}\t{0:.2f} C".format(code, value))

print("\nLeast accurate high temp. predictions:")
for i in range(1, topX + 1):
	absValue, value, code = averageDifference[-i]
	print("{}\t{0:.2f} C".format(code, value))

print("\nLowest variance in high temp. prediction accuracy:")
for i in range(topX):
	value, code = differenceVariance[i]
	print("{}\t{0:.2f} C^2".format(code, value))

print("\nHighest variance in high temp. prediction accuracy:")
for i in range(1, topX + 1):
	value, code = differenceVariance[-i]
	print("{}\t{0:.2f} C^2".format(code, value))