import psycopg2 as db
import time
from joblib import load
import numpy as np

model = load('best_model')
pca = load('pca')
imp1 = load('imputer_c1_c10')
imp2 = load('imputer_c11_c13')
conn = db.connect("dbname='technicaltest' user='mahdi' host='localhost' password='pass'")
conn.autocommit = True
cur = conn.cursor()
for i in range(15120):
    cur.execute("select * from data where Id = " + str(i))
    rows = cur.fetchall()

    for row in rows:
        data = np.array(row[1:])[np.newaxis,:]
        dat1 = imp1.transform(data[:,:10])
        dat2 = imp2.transform(data[:,10:13])
        data = np.hstack((dat1, dat2))
        data = pca.transform(data)
        res = model.predict(data)
        cur.execute('insert into result values(' + str(row[0]) + ', ' + str(res[0]) + ')')
    time.sleep(1)

conn.close()
