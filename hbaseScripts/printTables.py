import happybase as hb

conn = hb.Connection()

print(conn.tables())

conn.close()
