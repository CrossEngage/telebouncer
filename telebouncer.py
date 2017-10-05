#!/usr/bin/env python3

import argparse
import psycopg2
import itertools
import socket

# Possible commands to send to pgBouncer according to https://pgbouncer.github.io/usage.html#admin-console
queries = [
    'active_sockets',
    'clients',
    'databases',
    'fds',
    'lists',
    'mem',
    'pools',
    'servers',
    'sockets',
    'stats',
]

# A mapping for the columns of each query to assign them on the tag_set or field_set
# This should be changed according to your needs
mapping = {
    'active_sockets': {
        'type'         : 'tag',
        'user'         : 'tag',
        'database'     : 'tag',
        'state'        : 'tag',
        'addr'         : 'field',
        'port'         : 'field',
        'local_addr'   : 'field',
        'local_port'   : 'field',
        'connect_time' : 'field',
        'request_time' : 'field',
        'ptr'          : 'off',
        'link'         : 'off',
        'remote_pid'   : 'off',
        'tls'          : 'off',
        'recv_pos'     : 'field',
        'pkt_pos'      : 'field',
        'pkt_remain'   : 'field',
        'send_pos'     : 'field',
        'send_remain'  : 'field',
        'pkt_avail'    : 'field',
        'send_avail'   : 'field',
    },
    'clients': {
        'type'         : 'tag',
        'user'         : 'tag',
        'database'     : 'tag',
        'state'        : 'tag',
        'addr'         : 'field',
        'port'         : 'field',
        'local_addr'   : 'off',
        'local_port'   : 'off',
        'connect_time' : 'field',
        'request_time' : 'field',
        'ptr'          : 'off',
        'link'         : 'off',
        'remote_pid'   : 'off',
        'tls'          : 'off',
    },
    'databases': {
        'name'                : 'tag',
        'host'                : 'field',
        'port'                : 'field',
        'database'            : 'field',
        'force_user'          : 'off',
        'pool_size'           : 'field',
        'reserve_pool'        : 'field',
        'pool_mode'           : 'tag',
        'max_connections'     : 'field',
        'current_connections' : 'field',
    },
    'fds': {
        'fd'              : 'tag',
        'task'            : 'tag',
        'user'            : 'tag',
        'database'        : 'tag',
        'addr'            : 'field',
        'port'            : 'field',
        'cancel'          : 'off',
        'link'            : 'off',
        'client_encoding' : 'off',
        'std_strings'     : 'off',
        'datestyle'       : 'off',
        'timezone'        : 'field',
        'password'        : 'off',
    },
    'lists': {
        'list'  : 'tag',
        'items' : 'field',
    },
    'mem': {
        'name'     : 'tag',
        'size'     : 'field',
        'used'     : 'field',
        'free'     : 'field',
        'memtotal' : 'field',
    },
    'pools': {
        'database'   : 'tag',
        'user'       : 'tag',
        'cl_active'  : 'field',
        'cl_waiting' : 'field',
        'sv_active'  : 'field',
        'sv_idle'    : 'field',
        'sv_used'    : 'field',
        'sv_tested'  : 'field',
        'sv_login'   : 'field',
        'maxwait'    : 'field',
        'pool_mode'  : 'tag',
    },
    'servers': {
        'type'         : 'tag',
        'user'         : 'tag',
        'database'     : 'tag',
        'state'        : 'tag',
        'addr'         : 'field',
        'port'         : 'field',
        'local_addr'   : 'field',
        'local_port'   : 'field',
        'connect_time' : 'field',
        'request_time' : 'field',
        'ptr'          : 'off',
        'link'         : 'off',
        'remote_pid'   : 'off',
        'tls'          : 'off',
    },
    'sockets': {
        'type'         : 'tag',
        'user'         : 'tag',
        'database'     : 'tag',
        'state'        : 'tag',
        'addr'         : 'field',
        'port'         : 'field',
        'local_addr'   : 'field',
        'local_port'   : 'field',
        'connect_time' : 'field',
        'request_time' : 'field',
        'ptr'          : 'off',
        'link'         : 'off',
        'remote_pid'   : 'off',
        'tls'          : 'off',
        'recv_pos'     : 'field',
        'pkt_pos'      : 'field',
        'pkt_remain'   : 'field',
        'send_pos'     : 'field',
        'send_remain'  : 'field',
        'pkt_avail'    : 'field',
        'send_avail'   : 'field',
    },
    'stats': {
        'database'         : 'tag',
        'total_requests'   : 'field',
        'total_received'   : 'field',
        'total_sent'       : 'field',
        'total_query_time' : 'field',
        'avg_req'          : 'field',
        'avg_recv'         : 'field',
        'avg_sent'         : 'field',
        'avg_query'        : 'field',
    },
}

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

server = socket.gethostname()

measurement = 'pgbouncer_' + qtype

for item in res:
    # tlist is a list of "key=value" strings for the tag_set
    # flist is a list of "key=value" string for the field_set
    tlist = ['server='+server]
    flist = []
    for k, v in item.items():
        # Create two key=value lists: one for tag_set and one for field_set
        if mapping[qtype][k] == 'tag':
            # Goes to the tlist
            tlist.append("=".join([k, str(v)]))
        elif mapping[qtype][k] == 'field':
            # Goes to the flist
            flist.append("=".join([k, str(v)]) if isinstance(v, (int, float)) else "=".join([k, '"' + str(v) + '"']))
        else:
            # Ignore columns with 'off' on the mapping
            continue
    # Create the tag_set and field_set joining the "key=value" items from their respective lists
    tag_set = ",".join([kvitem for kvitem in tlist])
    field_set = ",".join([kvitem for kvitem in flist])
    # Merge it all together in one line
    output = ",".join([measurement, tag_set])
    output = " ".join([output, field_set])
    # Print each result on its own line the way InfluxDB expects them
    print(output)
