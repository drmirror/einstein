#!/usr/local/bin/python3

import sys
import time
from random import random
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/?replicaSet=rs0")

coll = client["test"]["coll"]
coll = coll.with_options(write_concern=pymongo.write_concern.WriteConcern(w=2))

num_docs = int(sys.argv[1])
batch_size = int(sys.argv[2])

coll.drop()
coll.insert_many([{"_id":q, "a":-1} for q in range(1000,num_docs,1000)])
time.sleep(0.5)

totaltime = 0
errors = []

print ("starting run")

def get_document(batch, ident):
    for i in batch:
        if i._doc["_id"] == ident:
            return i._doc

for i in range(0,num_docs,batch_size):
    batch = [ pymongo.InsertOne({ "_id" : j, "a" : random()}) for j in range(i, i+batch_size) ]
    start = time.time()
    try:
        coll.bulk_write(batch, ordered=False)
    except pymongo.errors.BulkWriteError as e:
        for x in e.details[u'writeErrors']:
            error_id = x[u'op']['_id']
            errors.append(get_document(batch,error_id))
    totaltime += time.time() - start
    if i % 1000 == 0:
        avg = totaltime * batch_size / (i + batch_size)
        ops = batch_size / avg
        print ("i = %7d, errors = %4d, avg = %f, ops/s = %.0f"
               % (i + batch_size, len(errors), avg, ops))

client.close()
