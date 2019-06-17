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

asyncio.get_event_loop().run_until_complete(coll.drop())
asyncio.get_event_loop().run_until_complete(coll.insert_many(
    [{"_id":q, "a":-1} for q in range(1000,num_docs,1000)]))
time.sleep(0.5)

async def insert_one(coll, i):
    doc = { "_id" : i, "a" : random() }
    try:
        await coll.insert_one(doc)
        return None
    except pymongo.errors.DuplicateKeyError as e:
        return doc

async def main(coll, num_docs):
    results = await asyncio.gather(*[insert_one(coll, i) for i in range(num_docs)])
    return list(filter(lambda x : x != None, results))

start = time.time()
errors = asyncio.get_event_loop().run_until_complete(main(coll, num_docs))
end = time.time()

print ("gathered all tasks in %f seconds, %f ops/s" % (end-start, num_docs / (end-start)))
print ("#errors = %d" % (len(errors)))
