import typer
import yaml
import os
from init import Init
from publisher import Publisher
from subscriber import Subscriber


# Read config file
def readConfigFile(filepath):
    if not os.path.isfile(filepath):
        print(f"Error: File '{filepath}' does not exist.")
        exit(1)

    with open(filepath, "r") as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print(f"Error: Invalid YAML format in file '{filepath}': {str(e)}")
            exit()


main = typer.Typer()


# Create changelog and triggers
@main.command()
def install(file: str = "config.yml"):
    config = readConfigFile(file)
    init = Init(
        dbHost=config["database"]["host"],
        dbPort=config["database"]["port"],
        dbUser=config["database"]["user"],
        dbPass=config["database"]["password"],
        dbName=config["database"]["name"],
        kafkaBrokers=config["kafka"]["brokers"],
    )
    init.databaseConnection()
    if config["type"] == "master":
        init.createTableChangelog()
        init.createTriggerTable()
    elif config["type"] == "slave":
        init.truncateChangelogTable()
    else:
        print(f'Error: Unknown type: {config["type"]}')
        exit(1)


# Publish database changelog to slave
@main.command()
def publish(file: str = "config.yml"):
    config = readConfigFile(file)
    if config["type"] != "master":
        print("Error: Only master type can publish")
        exit(1)

    publisher = Publisher(
        dbHost=config["database"]["host"],
        dbPort=config["database"]["port"],
        dbUser=config["database"]["user"],
        dbPass=config["database"]["password"],
        dbName=config["database"]["name"],
        kafkaBrokers=config["kafka"]["brokers"],
        delay=config["publish_delay"],
    )
    publisher.databaseConnection()
    publisher.createProducer()
    publisher.publish()


@main.command()
def subscribe(file: str = "config.yml"):
    config = readConfigFile(file)
    if config["type"] != "slave":
        print("Error: Only slave type can publish")
        exit(1)

    subscriber = Subscriber(
        dbHost=config["database"]["host"],
        dbPort=config["database"]["port"],
        dbUser=config["database"]["user"],
        dbPass=config["database"]["password"],
        dbName=config["database"]["name"],
        kafkaBrokers=config["kafka"]["brokers"],
    )
    subscriber.databaseConnection()
    subscriber.subscribe()


if __name__ == "__main__":
    main()
