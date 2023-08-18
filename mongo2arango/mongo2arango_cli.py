import click
from simplejson import dumps, loads
import orjson
from arango_orm import Database, Collection
from arango_orm.fields import Field
from arango import ArangoClient
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import queue
import os
from dotenv import load_dotenv
import glob
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Configure the logger
log_file = "./logs/migration.log"
log_handler = RotatingFileHandler(log_file, maxBytes=5e6, backupCount=4)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.DEBUG)  # Set logging level to WARNING


def is_mongo_date(value):
    return isinstance(value, dict) and ("$date" in value or "$numberLong" in value)


def convert_mongo_date(value):
    try:
        if "$date" in value:
            # Ensure that value["$date"] is a string
            if not isinstance(value["$date"], str):
                logging.error(
                    f"Expected a string for MongoDB date, but got {type(value['$date'])}: {value['$date']}"
                )
                return None

            # Extract the date up to the seconds (ignoring milliseconds)
            date_str = value["$date"][:19]

            # Convert to datetime
            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")

            # Return the date in ISO format string with '.000Z' appended
            return dt.isoformat() + ".000Z"
        elif "$numberLong" in value:
            # Convert the $numberLong value to a datetime
            timestamp = int(value["$numberLong"]) / 1000  # Convert to seconds
            dt = datetime.utcfromtimestamp(timestamp)

            # Return the date in ISO format string with '.000Z' appended
            return dt.isoformat() + ".000Z"
    except Exception as e:
        logging.error(f"Error converting MongoDB date: {e}")
        return None


def bulk_migration(db, dir, key="_id"):
    logging.info(f"Starting bulk migration for directory: {dir}")  # Debug log
    try:
        collection_dirs = [
            sub_dir
            for sub_dir in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, sub_dir))
        ]

        for collection_dir in collection_dirs:
            col = collection_dir
            col_dir = os.path.join(dir, collection_dir)

            json_files = glob.glob(os.path.join(col_dir, "*.json"))

            for json_file in json_files:
                with open("mongo2arango_bulk_import.log", "a") as log_file:
                    log_file.write(f"Started importing from {json_file}\n")
                migrate_core(db, col, json_file, key)
                with open("mongo2arango_bulk_import.log", "a") as log_file:
                    log_file.write(f"Successfully imported from {json_file}\n")
    except Exception as e:
        logging.error(f"Error in bulk_migration: {e}")


# Define constants for the number of threads
PROCESSING_THREADS = 4
UPLOADING_THREADS = 4
THREAD_BATCH_SIZE = 1000
UPLOAD_BATCH_SIZE = 1000
MAX_DOCS_IN_MEMORY = THREAD_BATCH_SIZE * 10


@click.group()
def cli():
    pass


@cli.command()
def login():
    ARANGODB_HOST = click.prompt("Enter ArangoDB Host")
    ARANGODB_USER = click.prompt("Enter ArangoDB User")
    ARANGODB_PW = click.prompt("Enter ArangoDB Password", hide_input=True)

    home_directory = os.path.expanduser("~")
    env_path = os.path.join(home_directory, ".mongo2arango_env")

    with open(env_path, "w") as f:
        f.write(f"ARANGODB_HOST={ARANGODB_HOST}\n")
        f.write(f"ARANGODB_USER={ARANGODB_USER}\n")
        f.write(f"ARANGODB_PW={ARANGODB_PW}\n")

    click.echo("Credentials saved!")


home_directory = os.path.expanduser("~")
env_path = os.path.join(home_directory, ".mongo2arango_env")
load_dotenv(dotenv_path=env_path)

ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")


def migrate_core(db, col, input, key):
    logging.info(f"Starting migration for collection: {col}")
    try:
        # ArangoDB connection
        client = ArangoClient(hosts=ARANGODB_HOST, serializer=dumps, deserializer=loads)
        system_db = client.db("_system", username=ARANGODB_USER, password=ARANGODB_PW)
        if not system_db.has_database(db):
            system_db.create_database(db)

        _db = client.db(db, username=ARANGODB_USER, password=ARANGODB_PW)
        target_db = Database(_db)

        CollectionClass = type(
            col,
            (Collection,),
            {"__collection__": col, "_fields": {}},
        )

        if not target_db.has_collection(col):
            target_db.create_collection(CollectionClass)
            logging.info(f"Collection {col} created.")
        else:
            logging.info(
                f"Collection {col} already exists. Continuing with data insertion..."
            )

        # Define a function to process a batch of documents
        def process_batch(docs, structure=None, key=key):
            logging.debug(f"Processing batch of {len(docs)} documents.")
            docs_to_insert = []

            def recursive_date_conversion(data):
                if isinstance(data, dict):
                    for key, value in data.items():
                        if is_mongo_date(value):
                            data[key] = convert_mongo_date(value)
                        else:
                            recursive_date_conversion(value)
                elif isinstance(data, list):
                    for idx, item in enumerate(data):
                        if is_mongo_date(item):
                            data[idx] = convert_mongo_date(item)
                        else:
                            recursive_date_conversion(item)

            for doc in docs:
                if structure:
                    for key in structure:
                        if is_mongo_date(doc.get(key)):
                            doc[key] = convert_mongo_date(doc[key])
                else:
                    recursive_date_conversion(doc)

                CollectionClass._fields = {
                    k: Field(allow_none=True) for k in doc.keys()
                }
                doc["_key"] = doc["_id"]
                del doc[key]
                entity = CollectionClass(**doc)
                docs_to_insert.append(entity)
            return docs_to_insert

        # Load data from the specified JSON file
        with open(input, "rb") as f:
            all_data = orjson.loads(f.read())
            total_docs = len(all_data)

        # Split the JSON data into multiple parts for threads
        def split_data(items, batch_size):
            batch = []
            for item in items:
                batch.append(item)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        # Process and upload in chunks of MAX_DOCS_IN_MEMORY
        for start_idx in range(0, total_docs, MAX_DOCS_IN_MEMORY):
            data_chunk = all_data[start_idx : start_idx + MAX_DOCS_IN_MEMORY]
            processed_data = []

            # Dynamically set the length of the progress bar
            progress_length = min(MAX_DOCS_IN_MEMORY, len(data_chunk))

            # Start the progress bar with the dynamic length
            with click.progressbar(
                length=progress_length,
                label=f"Importing documents from collection {col}",
                fill_char=click.style("▌", fg="cyan"),
                empty_char=" ",
                show_percent=True,
                show_eta=True,
                show_pos=True,
                width=20,
            ) as bar:
                # Use a thread pool to process each part concurrently
                with ThreadPoolExecutor(max_workers=PROCESSING_THREADS) as executor:
                    futures = [
                        executor.submit(process_batch, batch)
                        for batch in split_data(data_chunk, THREAD_BATCH_SIZE)
                    ]
                    for future in futures:
                        processed_data.extend(future.result())
                        bar.update(
                            len(future.result())
                        )  # Update the progress bar by the number of processed documents

                # Now, upload the processed data using 2 threads
                with ThreadPoolExecutor(max_workers=UPLOADING_THREADS) as executor:
                    upload_futures = [
                        executor.submit(target_db.bulk_add, batch)
                        for batch in split_data(processed_data, UPLOAD_BATCH_SIZE)
                    ]
                    for future in upload_futures:
                        if future.exception():
                            logging.error(
                                f"Upload encountered an error: {future.exception()}"
                            )
                        else:
                            bar.update(
                                len(future.result())
                            )  # Update the progress bar by the number of uploaded documents

    except Exception as e:
        logging.error(f"Error in migrate_core: {e}")


@cli.command()
@click.option(
    "--db", "-d", required=False, type=str, help="Name of the target ArangoDB database."
)
@click.option(
    "--col",
    "-c",
    required=False,
    type=str,
    help="Name of the target ArangoDB collection.",
)
@click.option(
    "--input", "-i", required=False, type=str, help="Path to the input JSON file."
)
@click.option(
    "--key",
    "-k",
    required=False,
    type=str,
    default="_id",
    help="Name of the MongoDB field to map as the ArangoDB document key.",
)
def migrate(db=None, col=None, input=None, key="_id"):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not col:
        col = click.prompt("Name of the target ArangoDB collection", type=str)
    if not input:
        input = click.prompt("Path to the input JSON file", type=str)
    if not key:
        key = click.prompt(
            "Name of the MongoDB field to map as the ArangoDB document key",
            type=str,
            default="_id",
        )

    migrate_core(db, col, input, key)
    print("Migration completed!")


@cli.command()
@click.option(
    "--db", "-d", required=False, type=str, help="Name of the target ArangoDB database."
)
@click.option(
    "--dir",
    "-dr",
    required=False,
    type=str,
    help="Directory containing JSON files for bulk migration.",
)
@click.option(
    "--key",
    "-k",
    required=False,
    type=str,
    default="_id",
    help="Name of the field to use as the ArangoDB document key.",
)
def migrate_bulk(db=None, dir=None, key="_id"):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not dir:
        dir = click.prompt(
            "Directory containing JSON files for bulk migration", type=str
        )
    bulk_migration(db, dir, key)


@cli.command()
@click.option(
    "--db", "-d", required=False, type=str, help="Name of the target ArangoDB database."
)
@click.option(
    "--dir",
    "-dir",
    required=False,
    type=str,
    help="Directory containing JSON files for small-scale migration.",
)
@click.option(
    "--key",
    "-k",
    required=False,
    type=str,
    default="_id",
    help="Name of the field to use as the ArangoDB document key.",
)
def migrate_small(db=None, dir=None, key="_id"):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not dir:
        dir = click.prompt(
            "Directory containing JSON files for small-scale migration", type=str
        )

    json_files = glob.glob(os.path.join(dir, "*.json"))

    for json_file in json_files:
        col = os.path.splitext(os.path.basename(json_file))[0]

        with open("mongo2arango_small_migration.log", "a") as log_file:
            log_file.write(f"Started importing from {json_file}\n")

        migrate_core(db, col, json_file, key)

        with open("mongo2arango_small_migration.log", "a") as log_file:
            log_file.write(f"Successfully imported from {json_file}\n")


def check_env_vars():
    if not ARANGODB_HOST or not ARANGODB_USER or not ARANGODB_PW:
        click.echo(
            "Please run 'mongo2arango login' to set up your ArangoDB credentials."
        )
        exit(1)


def collection_exists(db_name, col_name):
    client = ArangoClient(hosts=ARANGODB_HOST, serializer=dumps, deserializer=loads)
    _db = client.db(db_name, username=ARANGODB_USER, password=ARANGODB_PW)
    return _db.has_collection(col_name)


if __name__ == "__main__":
    try:
        check_env_vars()
        cli()
    except Exception as e:
        logging.exception("Error in main execution:")
