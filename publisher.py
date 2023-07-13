from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from database import Database
import json


class Publisher(object):
    def __init__(self, dbHost, dbPort, dbUser, dbPass, dbName, kafkaBrokers, delay=1):
        # database
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.dbName = dbName
        # rabbitmq
        self.kafkaBrokers = kafkaBrokers
        self.producer: KafkaProducer
        self.topic = "auditlog"
        # publisher
        self.arrChangeLogID = []
        self.delay = delay

    def databaseConnection(self):
        self.db = Database(
            host=self.dbHost,
            port=self.dbPort,
            user=self.dbUser,
            password=self.dbPass,
            databaseName=self.dbName,
        )
        self.db.connect()

    def selectUnpublishedChangeLog(self):
        columns = ["changelog_id", "query", "table", "pk", "type", "created_at"]
        sql = ", ".join(map(lambda c: f"`{c}`", columns))
        sql = f"SELECT {sql} FROM audit_changelog WHERE published = 0"
        result = self.db.select(sql)
        changelogs = []
        for row in result:
            changelog = {}
            for i, c in enumerate(columns):
                changelog[c] = row[i]
            changelogs.append(changelog)
        return changelogs

    def updatePublishStatus(self, arrChangeLogID):
        sql = ", ".join(map(str, arrChangeLogID))
        sql = f"UPDATE audit_changelog SET published = 1 WHERE changelog_id IN ({sql})"
        self.db.update(sql)

    def createProducer(self):
        brokers = self.kafkaBrokers.split(",")
        brokers = [e.strip() for e in brokers]
        self.producer = KafkaProducer(retries=5, bootstrap_servers=brokers)

    def sendChangelogToKafka(self, changelog):
        self.producer.send(
            topic=self.topic, value=json.dumps(changelog, default=str).encode()
        )
        return True

    def flushProducer(self):
        self.producer.flush()

    def publish(self):
        eventTypes = {
            1: "INS",
            2: "UPD",
            3: "DEL",
        }
        while True:
            arrSuccessLogID = []
            changelogs = self.selectUnpublishedChangeLog()

            for log in changelogs:
                success = self.sendChangelogToKafka(log)
                if success:
                    eventType = eventTypes[log["type"]]
                    print(
                        f"[{log['changelog_id']}] {eventType} {log['table']}:{log['pk']} -> KAFKA"
                    )
                    arrSuccessLogID.append(log["changelog_id"])

            if len(arrSuccessLogID):
                self.updatePublishStatus(arrSuccessLogID)

            sleep(self.delay)
