import happybase as hb

conn = hb.Connection()

columnFamilies = { 'cf': dict() }

conn.create_table('probabilityOfPrecipitation', columnFamilies)
conn.create_table('maxTemperature', columnFamilies)
conn.create_table('quantitativePrecipitation', columnFamilies)

print('Created tables')
conn.close()