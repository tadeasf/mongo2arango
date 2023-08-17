import click
from simplejson import dumps, loads
import ijson
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
log_handler = RotatingFileHandler(
    log_file, maxBytes=5e6, backupCount=4
)  # 5MB per log file, keep 4 backups
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.DEBUG)


def is_mongo_date(value):
    return isinstance(value, dict) and "$date" in value


def convert_mongo_date(value):
    try:
        # Extract the date up to the seconds (ignoring milliseconds)
        date_str = value["$date"][:19]

        # Convert to datetime
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")

        # Return the date in ISO format string
        return dt.isoformat()

    except Exception as e:
        logging.error(f"Error converting MongoDB date: {e}")
        return None


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


def migrate_core(db, col, input, key, threads):
    logging.info(f"Starting migration for collection: {col}")
    try:
        # ArangoDB connection
        client = ArangoClient(hosts=ARANGODB_HOST, serializer=dumps, deserializer=loads)
        # Connect to the "_system" database to check if the specified database exists
        system_db = client.db("_system", username=ARANGODB_USER, password=ARANGODB_PW)
        if not system_db.has_database(db):
            system_db.create_database(db)

        # Now connect to the specified database
        _db = client.db(db, username=ARANGODB_USER, password=ARANGODB_PW)
        target_db = Database(_db)

        # Dynamically create the collection class based on the provided col
        CollectionClass = type(
            col,
            (Collection,),
            {"__collection__": col, "_fields": {}},
        )

        # Check if the collection exists in the database
        if not target_db.has_collection(col):
            # If it doesn't exist, create the collection
            target_db.create_collection(CollectionClass)
            logging.info(f"Collection {col} created.")
        else:
            logging.info(
                f"Collection {col} already exists. Continuing with data insertion..."
            )

        # # Register the collection with the database
        # if target_db.has_collection(col):
        #     logging.info(f"Collection {col} already exists. Skipping...")
        #     return
        # target_db.create_collection(CollectionClass)

        # Define a function to process a batch of documents
        def process_batch(docs, progress_queue):
            logging.debug(f"Processing batch of {len(docs)} documents.")
            docs_to_insert = []

            def recursive_date_conversion(data):
                """Recursively convert MongoDB date formats in nested structures."""
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
                # Recursively convert MongoDB dates
                recursive_date_conversion(doc)

                # Existing logic for _key and _fields
                CollectionClass._fields = {
                    k: Field(allow_none=True) for k in doc.keys()
                }
                if isinstance(doc[key], dict) and "$oid" in doc[key]:
                    doc["_key"] = doc[key]["$oid"]
                else:
                    doc["_key"] = doc[key]
                del doc[key]
                entity = CollectionClass(**doc)
                docs_to_insert.append(entity)
                if len(docs_to_insert) == BULK_IMPORT_BATCH_SIZE:
                    target_db.bulk_add(docs_to_insert)
                    progress_queue.put(len(docs_to_insert))
                    docs_to_insert = []
            if docs_to_insert:
                try:
                    target_db.bulk_add(docs_to_insert)
                    progress_queue.put(len(docs_to_insert))
                    # Comment out the following line to remove the unnecessary log
                    # logging.debug(f"Added {len(docs_to_insert)} documents to the database.")
                except Exception as e:
                    logging.error(f"Error adding documents to the database: {e}")

        # Load data from the specified JSON file
        filename = input

        # First, determine the total number of documents to import
        with open(filename, "r") as f:
            total_docs = sum(1 for _ in ijson.items(f, "item"))

        # Define the number of threads
        num_threads = threads

        # Define the two batch sizes
        THREAD_BATCH_SIZE = total_docs // num_threads
        BULK_IMPORT_BATCH_SIZE = num_threads * 1000

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

        # Create a thread-safe queue to track progress
        progress_queue = Queue()

        # Start the progress bar immediately
        with click.progressbar(
            length=total_docs,
            label=f"Importing documents from collection {col}",
            fill_char=click.style("▌", fg="cyan"),
            empty_char=" ",
            show_percent=True,
            show_eta=True,
            show_pos=True,
            width=20,
        ) as bar:
            # Use a thread pool to process each part concurrently
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                with open(filename, "r") as f:
                    # Stream items from the file using ijson
                    items_stream = ijson.items(f, "item")
                    for batch in split_data(items_stream, THREAD_BATCH_SIZE):
                        # Directly submit the batch for processing
                        futures.append(
                            executor.submit(process_batch, batch, progress_queue)
                        )

                # Continuously update the progress bar based on the progress_queue
                processed_docs = 0
                while processed_docs < total_docs:
                    try:
                        batch_processed = progress_queue.get(timeout=1)
                        bar.update(batch_processed)
                        processed_docs += batch_processed
                    except queue.Empty:
                        pass

                # After ThreadPoolExecutor block
                for future in futures:
                    if future.exception():
                        logging.error(
                            f"Thread encountered an error: {future.exception()}"
                        )
                    if not future.done():
                        logging.warning(f"Thread {future} did not complete its task.")

                # Continue updating the progress bar until all documents are processed
                while not progress_queue.empty():
                    batch_processed = progress_queue.get_nowait()
                    bar.update(batch_processed)
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
    help="Name of the field to use as the ArangoDB document key.",
)
@click.option(
    "--threads",
    "-t",
    required=False,
    type=int,
    default=4,
    help="Number of threads to use for migration.",
)
def migrate(db=None, col=None, input=None, key=None, threads=None):
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
    if not threads:
        threads = click.prompt(
            "Number of threads to use for migration", type=int, default=4
        )

    migrate_core(db, col, input, key, threads)
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
@click.option(
    "--threads",
    "-t",
    required=False,
    type=int,
    default=4,
    help="Number of threads to use for migration.",
)
def migrate_bulk(db=None, dir=None, key=None, threads=None):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not dir:
        dir = click.prompt(
            "Directory containing JSON files for bulk migration", type=str
        )
    if not threads:
        threads = click.prompt(
            "Number of threads to use for migration", type=int, default=4
        )
    try:
        # Get a list of subdirectories (collection names)
        collection_dirs = [
            sub_dir
            for sub_dir in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, sub_dir))
        ]

        for collection_dir in collection_dirs:
            col = collection_dir  # Collection name is the same as the directory name
            col_dir = os.path.join(dir, collection_dir)

            json_files = glob.glob(os.path.join(col_dir, "*.json"))
            total_files = len(json_files)
            processed_files = 0

            for json_file in json_files:
                with open("mongo2arango_bulk_import.log", "a") as log_file:
                    log_file.write(f"Started importing from {json_file}\n")
                migrate_core(db, col, json_file, key, threads)  # Using "_id" as the key
                with open("mongo2arango_bulk_import.log", "a") as log_file:
                    log_file.write(f"Successfully imported from {json_file}\n")
                processed_files += 1
    except Exception as e:
        logging.error(f"Error in migrate_bulk: {e}")


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
@click.option(
    "--threads",
    "-t",
    required=False,
    type=int,
    default=4,
    help="Number of threads to use for migration.",
)
def migrate_small(db=None, dir=None, key=None, threads=None):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not dir:
        dir = click.prompt(
            "Directory containing JSON files for small-scale migration", type=str
        )
    if not threads:
        threads = click.prompt(
            "Number of threads to use for migration", type=int, default=4
        )

    json_files = glob.glob(os.path.join(dir, "*.json"))
    total_files = len(json_files)
    processed_files = 0

    for json_file in json_files:
        col = os.path.splitext(os.path.basename(json_file))[
            0
        ]  # Extract collection name from file name

        with open("mongo2arango_small_migration.log", "a") as log_file:
            log_file.write(f"Started importing from {json_file}\n")

        migrate_core(db, col, json_file, key, threads)  # Using "_id" as the key

        with open("mongo2arango_small_migration.log", "a") as log_file:
            log_file.write(f"Successfully imported from {json_file}\n")

        processed_files += 1


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
