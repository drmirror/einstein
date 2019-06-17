#!/usr/local/bin/python3

import sys
import time
from random import random
import asyncio
import motor
import motor.motor_asyncio
import pymongo

client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017/?replicaSet=rs0")
coll = client["test"]["coll"]
coll = coll.with_options(write_concern=pymongo.write_concern.WriteConcern(w=2))

num_docs = int(sys.argv[1])
batch_size = int(sys.argv[2])

asyncio.get_event_loop().run_until_complete(coll.drop())
asyncio.get_event_loop().run_until_complete(coll.insert_many(
    [{"_id":q, "a":-1} for q in range(1000,1000000,1000)]))
time.sleep(0.5)

def get_document(batch, ident):
    for i in batch:
        if i._doc["_id"] == ident:
            return i._doc

async def bulk_write(coll, i, batch_size):
    batch = [ pymongo.InsertOne({ "_id" : q, "a" : random()}) for q in range(i, i+batch_size) ] 
    try:
        await coll.bulk_write(batch, ordered=False)
        return None
    except pymongo.errors.BulkWriteError as e:
        result = []
        for x in e.details[u'writeErrors']:
            error_id = x[u'op']['_id']
            result.append(get_document(batch,error_id))
        return result
    
async def main(coll, num_docs, batch_size):
    tasks = [ bulk_write(coll, x, batch_size) for x in range(0, num_docs, batch_size) ]
    results = await asyncio.gather(*tasks)
    results = list(filter(lambda x : x != None, results))
    # flatten the list, courtesy stack overflow
    return [item for sublist in results for item in sublist]
    
start = time.time()
errors = asyncio.get_event_loop().run_until_complete(main(coll, num_docs, batch_size))
end = time.time()

print ("gathered all tasks in %f seconds, %.0f ops/s" % (end-start, num_docs / (end-start)))
print ("#errors = %d" % (len(errors)))
