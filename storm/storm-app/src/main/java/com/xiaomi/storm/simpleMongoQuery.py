#!/usr/bin env python

from pymongo import Connection
from bson import json_util  
import ast
import json
import os

MONGO_ADDR = '10.201.19.11'
MONGO_PORT = 8777
MONGO_DBNAME = 'micloud'
MONGO_COLL = 'gallery'

STORM_RECORD_COUNT = 'status_code_count'

PERF_COUNTER_PUSH = '''echo "update storm_mongo status_code_%s `date '+%%s'` %s GAUGE" | nc perfcounter.miliao.srv 4444'''

conn = Connection(MONGO_ADDR, MONGO_PORT)
coll = conn[MONGO_DBNAME][MONGO_COLL]
mongo_dict = {}

for docs in coll.find().sort([('_id',-1)]).skip(1).limit(1):
    if docs:
        mongo_json = json.dumps(docs, default=json_util.default)
        mongo_dict = ast.literal_eval(mongo_json)

mongo_record = ast.literal_eval(mongo_dict['record'])
if mongo_record:
    for status_code ,value in mongo_record.iteritems():
        #print {status_code:value[STORM_RECORD_COUNT]}
        #print PERF_COUNTER_PUSH % (status_code, value[STORM_RECORD_COUNT])
        os.system(PERF_COUNTER_PUSH % (status_code, value[STORM_RECORD_COUNT]))

