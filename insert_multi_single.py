#!/usr/local/bin/python3

import multiprocessing
from multiprocessing import Process

import sys
import time
from random import random
import pymongo


def do_insert(start_id, increment, end_id, errors):
    client = pymongo.MongoClient(
        "mongodb://localhost:27017/?replicaSet=rs0"
    )
    coll = client["test"]["coll"].with_options(
        write_concern=pymongo.write_concern.WriteConcern(w=2)
    )
    for i in range(start_id,end_id,increment):
        try:
            doc = { "_id" : i, "a" : random() }
            coll.insert_one(doc)
        except pymongo.errors.DuplicateKeyError as e:
            errors.append(doc)
    client.close()

if __name__ == '__main__':
    num_docs = int(sys.argv[1])
    num_processes = int(sys.argv[2])

    client = pymongo.MongoClient(
        "mongodb://localhost:27017/?replicaSet=rs0"
    )
    coll = client["test"]["coll"].with_options(
        write_concern=pymongo.write_concern.WriteConcern(w=2)
    )
    client.close()

    coll.drop()
    coll.insert_many([{"_id":q, "a":-1} for q in range(1000,num_docs,1000)])
    time.sleep(0.5)
    
    m = multiprocessing.Manager()
    errors = m.list()

    processes = []
    for i in range(0,num_processes):
        processes.append(Process(target=do_insert, args=(i,num_processes,num_docs, errors)))

    start_time = time.time()
    for p in processes: p.start()
    for p in processes: p.join()
    end_time = time.time()

    delta = end_time - start_time
    print("%d documents in %f seconds, %f op/s" % (num_docs, delta, num_docs / delta))
    print("errors: %d" % (len(errors)))
    


