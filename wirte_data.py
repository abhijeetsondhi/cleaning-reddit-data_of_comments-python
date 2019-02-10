import sqlite3
import pandas as pd


conn = sqlite3.connect('2006-05.db')
c = conn.cursor()
l_unix = 0

data = pd.read_sql("SELECT * FROM parent_reps WHERE unix > {} AND parent NOT NULL ORDER BY unix ASC LIMIT 2000".format(l_unix),conn)
l_unix = data.tail(1)['unix'].values
l_unixt = l_unix[0]


with open("test_data.from","a",encoding='utf8') as record:
    for d in data['parent'].values:
        record.write(d+'\n')

with open("test_data.to","a",encoding='utf8') as record:
    for d in data['comment'].values:
        record.write(d+'\n')

data2 = pd.read_sql("SELECT * FROM parent_reps WHERE unix > {} AND parent NOT NULL ORDER BY unix ASC".format(l_unixt),conn)

with open("train_data.from","a",encoding='utf8') as record:
    for d in data2['parent'].values:
        record.write(d+'\n')

with open("train_data.to","a",encoding='utf8') as record:
    for d in data2['comment'].values:
        record.write(d+'\n')
