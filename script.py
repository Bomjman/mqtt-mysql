#!/usr/bin/env python

import os, time
import paho.mqtt.client as mqtt
from urllib.parse import urlparse
import mysql.connector as mysql

db = mysql.connect( user='mqtt', password='secret', database='mydb')
cursor = db.cursor()
broker_topic = [("Bailey001", 0)]

def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload.decode('utf-8')))

    sql = "INSERT INTO topics(topic, msg, time) VALUES(%s,%s, %s)"
    datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    try:
        cursor.execute(sql, [msg.topic, str(msg.payload.decode('utf-8')), datetime])
        print('Successfully Added record to mysql')
        db.commit()
    except mysql.Error as error:
        print("Error: {}".format(error))
        db.rollback()

# def on_publish(mosq, obj, mid):
#     print("mid: "+str(mid))


def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
#client.on_publish = on_publish
client.on_subscribe = on_subscribe

# Parse CLOUDMQTT_URL
url_str = os.environ.get('CLOUDMQTT_URL')
url = urlparse(url_str)

# Connect
client.username_pw_set(url.username, url.password)
client.connect(url.hostname, url.port)

# Start subscribe, with QoS level 0
client.subscribe(broker_topic)

#client.publish("Bailey001", "my local time")
# Continue the network loop
rc = 0
while rc == 0:
    rc = client.loop()
print("rc: " + str(rc))

# Disconnect from server
print('Dissconnected, done.')
db.close()
