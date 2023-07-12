import typer
import yaml
import os
from init import Init
from publisher import Publisher

# Read config file
def readConfigFile(filepath):
    if not os.path.isfile(filepath):
        print(f"Error: File '{filepath}' does not exist.")
        exit(1)
    
    with open(filepath, 'r') as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print(f"Error: Invalid YAML format in file '{filepath}': {str(e)}")
            exit()


main = typer.Typer()

# Create changelog and triggers
@main.command()
def install(filepath: str = "config.yml"):
    config = readConfigFile(filepath)
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
        init.dropChangelogTable()
        init.dropTriggerAuditLog()
    else:
        print(f'Error: Unknown type: {config["type"]}')
        exit(1)

# Publish database changelog to slave
@main.command()
def publish(filepath: str = "config.yml"):
    config = readConfigFile(filepath)
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
        delay=config["publish_delay"]
    )
    publisher.databaseConnection()
    publisher.createProducer()
    publisher.publish()


@main.command()
def subscribe():
    typer.echo(f"Consume")

if __name__ == "__main__":
    main()
