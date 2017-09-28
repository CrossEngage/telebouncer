# telebouncer.py

Python3 script to collect metrics from pgBouncer and print InfluxDB compatible output

This tool is meant to be used with Telegraf's `inputs.exec` plugin.

Use the `mapping` dictionary inside the script to set where each column from the query results go to: `tag_set` or `field_set` (or don't send it at all).
