from database import Database


class Init(object):
    def __init__(self, dbHost, dbPort, dbUser, dbPass, dbName, kafkaBrokers):
        # database
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.dbName = dbName
        # rabbitmq
        self.kafkaBrokers = kafkaBrokers

    def databaseConnection(self):
        self.db = Database(
            host=self.dbHost,
            port=self.dbPort,
            user=self.dbUser,
            password=self.dbPass,
            databaseName=self.dbName,
        )
        self.db.connect()

    def createTableChangelog(self):
        self.db.execute(
            """
      CREATE TABLE IF NOT EXISTS `audit_changelog` (
        `changelog_id` bigint(20) NOT NULL AUTO_INCREMENT,
        `query` text,
        `table` varchar(255) DEFAULT NULL,
        `pk` bigint(20) DEFAULT NULL,
        `type` tinyint(4) DEFAULT NULL,
        `published` tinyint(4) DEFAULT 0,
        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`changelog_id`)
      )
      """
        )
        print("[+] Auit changelog table created")

    def getTableName(self):
        res = self.db.select(
            'SELECT table_name FROM information_schema.tables\
      WHERE table_schema = "%s" AND (table_name NOT IN ("audit_changelog", "testing"))'
            % (self.dbName)
        )
        return res

    def getColumnTable(self, tableName):
        res = self.db.select(
            'SELECT COLUMN_NAME\
      FROM INFORMATION_SCHEMA.COLUMNS\
      WHERE TABLE_SCHEMA = "%s" AND TABLE_NAME = "%s" AND COLUMN_KEY <> "PRI"'
            % (self.dbName, tableName)
        )
        return res

    def getPrimaryKeyName(self, tableName):
        res = self.db.select(
            "SELECT key_column_usage.column_name\
      FROM information_schema.key_column_usage\
      WHERE table_schema = SCHEMA()\
      AND constraint_name = 'PRIMARY'\
      AND table_name = '%s'"
            % tableName
        )
        return res[0][0]

    def truncateTable(self, table):
        self.db.execute("TRUNCATE %s" % table)

    def dropTable(self, table):
        self.db.execute("DROP TABLE IF EXISTS %s" % table)

    def dropTrigger(self, trigger):
        self.db.execute("DROP TRIGGER IF EXISTS %s" % trigger)

    def createTriggerTable(self):
        print("[*] Creating table triggers..")
        tables = self.getTableName()
        for table in tables:
            columns = self.getColumnTable(table[0])
            primaryKey = self.getPrimaryKeyName(table[0])
            self.createTriggerAfterInsert(table[0], primaryKey, columns)
            self.createTriggerAfterUpdate(table[0], primaryKey, columns)
            self.createTriggerAfterDelete(table[0], primaryKey)

    def createTriggerAfterInsert(self, tableName, primaryKey, columns):
        triggerName = "auditlog_insert_" + tableName
        self.db.execute("DROP TRIGGER IF EXISTS %s" % triggerName)
        query = f"CREATE TRIGGER `{triggerName}` AFTER INSERT ON `{tableName}` FOR EACH ROW\
      INSERT INTO audit_changelog(`query`, `table`, `pk`, `type`) VALUES"
        queryValue = f'(CONCAT("INSERT INTO {tableName} ('
        columnsLength = len(columns)
        for i in range(columnsLength):
            queryValue += columns[i][0]
            if i < columnsLength - 1:
                queryValue += ", "
        queryValue += ") VALUES ("
        for i in range(columnsLength):
            queryValue += "'\", NEW." + columns[i][0] + ",\"'"
            if i < columnsLength - 1:
                queryValue += ", "
        queryValue += f')"), "{tableName}", NEW.{primaryKey}, 1);'
        self.db.execute(query + queryValue)
        print(f"[+] {tableName} : {triggerName}")

    def createTriggerAfterUpdate(self, tableName, primaryKey, columns):
        triggerName = "auditlog_update_" + tableName
        self.db.execute("DROP TRIGGER IF EXISTS %s" % triggerName)
        query = f"CREATE TRIGGER `{triggerName}` AFTER UPDATE ON `{tableName}` FOR EACH ROW BEGIN \
      INSERT INTO audit_changelog(`query`, `table`, `pk`, `type`) VALUES"
        queryValue = f'(CONCAT("UPDATE {tableName} SET '
        columnsLength = len(columns)
        for i in range(columnsLength):
            queryValue += columns[i][0] + " = " + "'\", NEW." + columns[i][0] + ",\"'"
            if i < columnsLength - 1:
                queryValue += ", "
        queryValue += f" WHERE {primaryKey} = "
        queryValue += "'\", OLD." + primaryKey + ",\"'"
        queryValue += f'"), "{tableName}", NEW.{primaryKey}, 2); END;'
        self.db.execute(query + queryValue)
        print(f"[+] {tableName} : {triggerName}")

    def createTriggerAfterDelete(self, tableName, primaryKey):
        triggerName = "auditlog_delete_" + tableName
        self.db.execute(f"DROP TRIGGER IF EXISTS {triggerName}")
        query = f'CREATE TRIGGER `{triggerName}` BEFORE DELETE ON `{tableName}` \
          FOR EACH ROW INSERT INTO audit_changelog(`query`, `table`, `pk`, `type`)\
          VALUES (CONCAT("DELETE FROM {tableName} WHERE {primaryKey}=", OLD.{primaryKey}), "{tableName}", OLD.{primaryKey}, 3);\
      '
        self.db.execute(query)
        print(f"[+] {tableName} : {triggerName}")

    def dropTriggerAuditLog(self):
        print("[*] Deleting table triggers..")
        tables = self.getTableName()
        for table in tables:
            triggerCaption = ["insert", "update", "delete"]
            for caption in triggerCaption:
                triggerName = f"auditlog_{caption}_{table[0]}"
                self.dropTrigger(triggerName)
                print(f"[-] {table[0]} : {triggerName}")

    def dropChangelogTable(self):
        self.dropTable("audit_changelog")
        print("[-] Audit changelog table deleted")

    def truncateChangelogTable(self):
        self.truncateTable("audit_changelog")
        print("[-] Audit changelog table truncated")
