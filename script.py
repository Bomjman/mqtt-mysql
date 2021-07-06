#!/usr/bin/env python

import os, json, time
import paho.mqtt.client as mqtt
from urllib.parse import urlparse
import mysql.connector as mysql

db = mysql.connect( user='mqtt', password='secret', database='mydb')
cursor = db.cursor()
broker_topic = [("hello/#", 0), ("/#", 0)]

def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    vars_to_sql = []
    keys_to_sql = []
    list = []

    list = json.loads(msg.payload)

    for key, value in list.iteritems():
        print("")
        print(key, value)
        if key == 'tst':
            print("time found")
            print (value)
            value = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(value)))
            print (value)

        value_type = type(value)
        if value_type is not dict:
            print("value_type is not dict")
            if value_type is unicode:
                print("value_type is unicode")
                vars_to_sql.append(value.encode('ascii', 'ignore'))
                keys_to_sql.append(key.encode('ascii', 'ignore'))
            else:
                print("value_type is not unicode")
                vars_to_sql.append(value)
                keys_to_sql.append(key)

    print("topic", msg.topic)
    addtopic = 'topic'
    vars_to_sql.append(msg.topic.encode('ascii', 'ignore'))
    keys_to_sql.append(addtopic.encode('ascii', 'ignore'))

    keys_to_sql = ', '.join(keys_to_sql)

    try:
       # Execute the SQL command
       # change locations to the table you are using
       queryText = "INSERT INTO locations(%s) VALUES %r"
       queryArgs = (keys_to_sql, tuple(vars_to_sql))
       cursor.execute(queryText % queryArgs)
       print('Successfully Added record to mysql')
       db.commit()

    except mysql.connector.Error as e:
        try:
            print("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
        except IndexError:
            print("MySQL Error: %s" % str(e))
        # Rollback in case there is any error
        db.rollback()
        print('ERROR adding record to MYSQL')

def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe

# Parse CLOUDMQTT_URL
url_str = os.environ.get('CLOUDMQTT_URL')
url = urlparse(url_str)

# Connect
client.username_pw_set(url.username, url.password)
client.connect(url.hostname, url.port)

# an array with some topics
# Start subscribe, with QoS level 0
client.subscribe(broker_topic)

# Continue the network loop
rc = 0
while rc == 0:
    rc = client.loop()
print("rc: " + str(rc))

# Disconnect from server
print('Dissconnected, done.')
db.close()
