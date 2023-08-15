import click
from simplejson import dumps, loads
import ijson
from arango_orm import Database, Collection
from arango_orm.fields import Field
from arango import ArangoClient
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv


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
    "-K",
    default="_id",
    type=str,
    help="Name of the MongoDB field to map as _key. Default is _id.",
)
@click.option(
    "--threads",
    "-t",
    default=4,
    type=int,
    help="Number of threads to use. Default is 4.",
)
@click.help_option("--help", "-h")
def migrate(db=None, col=None, input=None, key=None, threads=None):
    if not db:
        db = click.prompt("Name of the target ArangoDB database", type=str)
    if not col:
        col = click.prompt("Name of the target ArangoDB collection", type=str)
    if not input:
        while True:
            input = click.prompt("Path to the input JSON file", type=str)
            if os.path.exists(input) and input.endswith(".json"):
                break
            else:
                click.echo("Invalid JSON file path. Please provide a valid path.")
    if not key:
        key = click.prompt(
            "Name of the MongoDB field to map as _key", type=str, default="_id"
        )
    if not threads:
        while True:
            try:
                threads = click.prompt("Number of threads to use", type=int, default=4)
                if isinstance(threads, int) and threads > 0:
                    break
                else:
                    raise ValueError
            except ValueError:
                click.echo(
                    "Invalid number of threads. Please provide a positive integer."
                )

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

    # Register the collection with the database
    if not target_db.has_collection(col):
        target_db.create_collection(CollectionClass)

    # Define a function to process a batch of documents
    def process_batch(docs, progress_queue):
        docs_to_insert = []
        for doc in docs:
            CollectionClass._fields = {k: Field(allow_none=True) for k in doc.keys()}
            doc["_key"] = doc[key]
            del doc[key]
            entity = CollectionClass(**doc)
            docs_to_insert.append(entity)
            if len(docs_to_insert) == BULK_IMPORT_BATCH_SIZE:
                target_db.bulk_add(docs_to_insert)
                progress_queue.put(len(docs_to_insert))
                docs_to_insert = []
        if docs_to_insert:
            target_db.bulk_add(docs_to_insert)
            progress_queue.put(len(docs_to_insert))

    # Load data from the specified JSON file
    filename = input

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

    # Use a thread pool to process each part concurrently
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        with open(filename, "r") as f:
            items = list(ijson.items(f, "item"))
            for batch in split_data(items, THREAD_BATCH_SIZE):
                futures.append(executor.submit(process_batch, batch, progress_queue))

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
                batch_processed = progress_queue.get()
                processed_docs += batch_processed
                bar.update(batch_processed)

    print("Migration completed!")


def check_env_vars():
    if not ARANGODB_HOST or not ARANGODB_USER or not ARANGODB_PW:
        click.echo(
            "Please run 'mongo2arango login' to set up your ArangoDB credentials."
        )
        exit(1)


if __name__ == "__main__":
    check_env_vars()
    cli()
