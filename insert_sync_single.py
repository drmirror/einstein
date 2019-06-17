#!/usr/local/bin/python3

import sys
import time
from random import random
import pymongo

client = pymongo.MongoClient(
    "mongodb://localhost:27017/?replicaSet=rs0"
)

coll = client["test"]["coll"].with_options(
    write_concern=pymongo.write_concern.WriteConcern(w=2)
)

num_docs = int(sys.argv[1])

coll.drop()
coll.insert_many([{"_id":q, "a":-1} for q in range(1000,num_docs,1000)])
time.sleep(0.5)

totaltime = 0
errors = []

print ("starting run")

for i in range(num_docs):
    start = time.time()
    try:
        doc = { "_id" : i, "a" : random() }
        coll.insert_one(doc)
    except pymongo.errors.DuplicateKeyError as e:
        errors.append(doc)
    totaltime += time.time() - start
    if i > 0 and i % 1000 == 0:
        print ("i = %d, errors = %d, avg = %f ops/s = %.0f" % (i, len(errors), (totaltime / i), (i / totaltime)))

client.close()
