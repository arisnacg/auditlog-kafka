from kafka import KafkaConsumer
from database import Database
import json


class Subscriber(object):
    def __init__(self, dbHost, dbPort, dbUser, dbPass, dbName, kafkaBrokers, delay=1):
        # database
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.dbName = dbName
        # rabbitmq
        self.kafkaBrokers = kafkaBrokers
        self.topic = "auditlog"

    def databaseConnection(self):
        self.db = Database(
            host=self.dbHost,
            port=self.dbPort,
            user=self.dbUser,
            password=self.dbPass,
            databaseName=self.dbName,
        )
        self.db.connect()

    def subscribe(self):
        eventTypes = {
            1: "INS",
            2: "UPD",
            3: "DEL",
        }
        brokers = self.kafkaBrokers.split(",")
        brokers = [e.strip() for e in brokers]
        print(brokers)
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=brokers,
        )
        for msg in consumer:
            log = json.loads(msg.value)
            self.db.execute(log["query"])
            eventType = eventTypes[log["type"]]
            print(
                f"[{log['changelog_id']}] {eventType} {log['table']}:{log['pk']} <- KAFKA"
            )
