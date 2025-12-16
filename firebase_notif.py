import firebase_admin
from firebase_admin import credentials, messaging
import pandas as pd
import datetime
import rpy2.robjects as robj
import os

os.chdir("/srv/shiny-server/EnvizAPI")
cred = credentials.Certificate("EnvizServiceKey.json")  # Replace with your service account key file path
firebase_admin.initialize_app(cred)

clients = pd.read_csv("../Clients Usage Data/client_profiles.csv")
clients = clients[clients['notifEnable'] == True]
current_time = datetime.datetime.now()
current_hour = current_time.hour
if (len(clients) > 0):
    clients = clients[clients['notifHour'] == current_hour]
print(current_time)
print("Clients at this hour: " + str(len(clients)))


def send_notification(token, title, body):
    # Create a message
    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        token=token,
    )

    # Send a message to the device corresponding to the provided registration token
    response = messaging.send(message)
    print(response)


for index, row in clients.iterrows():
    user_token = row['deviceToken']  # The registration token of the device to which the message should be sent
    notification_title = "EnViz: Daily Report"
    user = row['user']

    print("Sending notification to " + user)

    yday = datetime.date.today() - datetime.timedelta(days=1)
    yday = yday.strftime("%Y-%m-%d")

    robj.r['source']('firebase_helper.R')
    result = robj.r['total_usage'](user, yday)

    result = str(result[0])

    send_notification(user_token, notification_title, result)