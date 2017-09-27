#!/usr/bin/env python3

import argparse
import psycopg2
import itertools
import socket

# Possible commands to send to pgBouncer according to https://pgbouncer.github.io/usage.html#admin-console
queries = [
    'active_sockets',
    'clients',
    'config',
    'databases',
    'fds',
    'lists',
    'mem',
    'pools',
    'servers',
    'sockets',
    'stats',
]

# host and port are optional as they have the defaults according to pgBouncer default installation values
# username is required as it must be a valid user from either stats_user or admin_users from pgBouncer's settings
# password is optional because if you have a ~/.pgpass file in place with proper values psycopg2 can get the password from there, as it is a wrapper for the libpq
parser = argparse.ArgumentParser(conflict_handler='resolve')
parser.add_argument("-h", "--host", help="pgBouncer host or IP (default: \"127.0.0.1\")", type=str, default='127.0.0.1')
parser.add_argument("-p", "--port", help="pgBouncer port (pgBouncer default: \"6432\")", type=str, default='6432')
parser.add_argument("-U", "--username", help="a valid user from pgBouncer \"stats_users\" or \"admin_users\" config", dest='user', type=str, required=True)
parser.add_argument("-W", "--password", help="password for username", type=str)
parser.add_argument("query", help="Which internal pgBouncer query to run", type=str, choices=queries)
args = parser.parse_args()

# We use the parameters passed on the command line to create the connection string
# And also add the dbname for pgBouncer's virtual database
params = vars(args)
params.update({'dbname': 'pgbouncer'})

# Extract the desired query
qtype = params.pop('query')

# Putting everything together for the connection string
conn_str = " ".join([k + '=' + v for k, v in params.items() if v is not None])

try:
    conn = psycopg2.connect(conn_str)
except:
    print("Error connecting to pgBouncer's database!")
    quit(1)

conn.autocommit = True
cur = conn.cursor()

query = " ".join(["SHOW", qtype.upper()])

try:
    cur.execute(query)
except:
    print("Error running query!")
    quit(2)

# To make it easier to parse later on we put the results together on a dictionary
cols = [desc[0] for desc in cur.description]
res = [dict(itertools.zip_longest(cols, row)) for row in cur.fetchall()]

cur.close()
conn.close()

# Desired output according to https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_tutorial/
# measurement,tag_set field_set

hostname = socket.gethostname()

measurement = 'pgbouncer'
tag_set = ",".join(['host='+hostname, 'query='+qtype])

for item in res:
    mlist = []
    for k, v in item.items():
        # Create a list for the metrics based on key=value
        mlist.append("=".join([k, str(v)]) if isinstance(v, (int, float)) else "=".join([k, '"' + str(v) + '"']))
    # Create the field_set joining the key=value items from the list on a single string
    field_set = ",".join([kvitem for kvitem in mlist])
    # Merge it all together in one line
    output = ",".join([measurement, tag_set])
    output = " ".join([output, field_set])
    # Print each result on its own line the way InfluxDB expects them
    print(output)
