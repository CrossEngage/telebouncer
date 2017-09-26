#!/usr/bin/env python3

import argparse
import psycopg2
import itertools
import pprint

queries = ['STATS', 'POOLS', 'DATABASES', 'SERVERS', 'CLIENTS']

parser = argparse.ArgumentParser(conflict_handler='resolve')
# parser.add_argument("", "", help="", nargs=, default='')
parser.add_argument("-h", "--host", help="pgBouncer host or IP (default: \"127.0.0.1\")", type=str, default='127.0.0.1')
parser.add_argument("-p", "--port", help="pgBouncer port (pgBouncer default: \"6432\")", type=str, default='6432')
parser.add_argument("-U", "--username", help="a valid user from pgBouncer \"stats_users\" config", type=str)
parser.add_argument("-W", "--password", help="password for username", type=str)
parser.add_argument("query", help="Which internal pgBouncer query to run", type=str, choices=queries)
args = parser.parse_args()

query = args.__dict__.pop('query')

conn_str = " ".join([k + '=' + v for k, v in args.__dict__.items() if v != None])

try:
    conn = psycopg2.connect(conn_str)
except:
    print("Error connecting to the database!")
    quit(1)

conn.autocommit = True
cur = conn.cursor()


for type in queries:
    query = " ".join(["SHOW", type])
    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    res = [dict(itertools.zip_longest(cols, row)) for row in cur.fetchall()]
    print(query + ":")
    pprint.pprint(res)
    print("\n")

cur.close()
conn.close()
