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

logging.basicConfig(
    filename="migration.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


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


def migrate_core(db, col, input, key, threads, update_option=False):
    # Save the current logging level
    current_log_level = logging.getLogger().getEffectiveLevel()

    # Set logging level to WARNING to suppress DEBUG and INFO messages
    logging.getLogger().setLevel(logging.WARNING)
    logging.info(f"Starting migration for collection: {col}")

    try:
        # ArangoDB connection
        client = ArangoClient(hosts=ARANGODB_HOST, serializer=dumps, deserializer=loads)
    except Exception as e:
        logging.error(f"Error initializing ArangoClient: {e}")
        return

    try:
        # Connect to the "_system" database to check if the specified database exists
        system_db = client.db("_system", username=ARANGODB_USER, password=ARANGODB_PW)
        if not system_db.has_database(db):
            system_db.create_database(db)
    except Exception as e:
        logging.error(f"Error connecting to _system database or creating database: {e}")
        return

    try:
        # Now connect to the specified database
        _db = client.db(db, username=ARANGODB_USER, password=ARANGODB_PW)
        target_db = Database(_db)
    except Exception as e:
        logging.error(f"Error connecting to target database: {e}")
        return

    try:
        # Dynamically create the collection class based on the provided col
        CollectionClass = type(
            col,
            (Collection,),
            {"__collection__": col, "_fields": {}},
        )

        # Register the collection with the database
        if not target_db.has_collection(col):
            target_db.create_collection(CollectionClass)
    except Exception as e:
        logging.error(f"Error creating or checking collection: {e}")
        return

    # Define a function to process a batch of documents
    def process_batch(docs, progress_queue):
        logging.debug(f"Processing batch of {len(docs)} documents.")
        docs_to_insert = []
        for doc in docs:
            try:
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
            except Exception as e:
                logging.error(f"Error processing document: {doc}. Error: {e}")
        if docs_to_insert:
            try:
                target_db.bulk_add(docs_to_insert)
                progress_queue.put(len(docs_to_insert))
                logging.debug(f"Added {len(docs_to_insert)} documents to the database.")
            except Exception as e:
                logging.error(f"Error adding documents to the database: {e}")

    # Load data from the specified JSON file
    filename = input

    try:
        # Load data from the specified JSON file and determine the total number of documents to import
        with open(filename, "r") as f:
            total_docs = sum(1 for _ in ijson.items(f, "item"))
    except Exception as e:
        logging.error(f"Error reading input file or counting documents: {e}")
        return

    # First, determine the total number of documents to import
    with open(filename, "r") as f:
        total_docs = sum(1 for _ in ijson.items(f, "item"))

    # Define the number of threads
    num_threads = threads

    # Define the two batch sizes
    THREAD_BATCH_SIZE = total_docs // num_threads
    BULK_IMPORT_BATCH_SIZE = 20000 // num_threads

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

    try:
        # Use a thread pool to process each part concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            # After ThreadPoolExecutor block
            for future in futures:
                if not future.done():
                    logging.warning(f"Thread {future} did not complete its task.")
            with open(filename, "r") as f:
                items = list(ijson.items(f, "item"))
                for batch in split_data(items, THREAD_BATCH_SIZE):
                    futures.append(
                        executor.submit(process_batch, batch, progress_queue)
                    )
    except Exception as e:
        logging.error(f"Error processing data with ThreadPoolExecutor: {e}")
        return

    # Aggregate the progress from all threads into a single progress bar
    processed_docs = 0

    with click.progressbar(
        length=total_docs,
        label="Importing documents",
        fill_char=click.style("▌", fg="cyan"),
        empty_char=" ",
        show_percent=True,
        show_eta=True,
        show_pos=True,
        width=20,
    ) as bar:
        while processed_docs < total_docs:
            try:
                logging.debug(
                    f"Current size of progress_queue: {progress_queue.qsize()}"
                )
                batch_processed = progress_queue.get(timeout=60)
                processed_docs += batch_processed
                bar.update(batch_processed)
            except queue.Empty:
                logging.error(
                    "Timeout waiting for progress_queue. Possible deadlock or thread issue."
                )
                break
    # Restore the original logging level
    logging.getLogger().setLevel(current_log_level)


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
def migrate(db=None, col=None, input=None, key=None, threads=None, update_option=False):
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

    migrate_core(db, col, input, key, threads, update_option)
    print("Migration completed!")


@cli.command()
@click.option(
    "--db", "-d", required=False, type=str, help="Name of the target ArangoDB database."
)
@click.option(
    "--dir",
    "-dir",
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
@click.option(
    "--update",
    "-u",
    "update_option",
    is_flag=True,
    help="Only add new documents if set.",
)
def migrate_bulk(db=None, dir=None, key=None, threads=None, update_option=False):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not dir:
        dir = click.prompt(
            "Directory containing JSON files for bulk migration", type=str
        )
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

    json_files = glob.glob(os.path.join(dir, "*.json"))
    total_collections = len(json_files)
    processed_collections = 0

    for json_file in json_files:
        col = os.path.splitext(os.path.basename(json_file))[0]
        if collection_exists(db, col):
            print(f"Collection {col} already exists. Skipping...")
            continue
        print(f"Processing collection: {col}")
        with open("mongo2arango_bulk_import.log", "a") as log_file:
            log_file.write(f"Started importing {col}\n")
        migrate_core(db, col, json_file, key, threads, update_option=True)
        with open("mongo2arango_bulk_import.log", "a") as log_file:
            log_file.write(f"Successfully imported {col}\n")
        processed_collections += 1
        print(f"Processed {processed_collections}/{total_collections} collections.")


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
    check_env_vars()
    cli()
