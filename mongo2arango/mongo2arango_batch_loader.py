import os
import click
from mongo2arango_cli import bulk_migration
import logging
import multiprocessing
from tqdm import tqdm


def migrate_single_directory(input_dir, progress_bar):
    db = "daytrip"  # Hardcoded database name
    bulk_migration(db=db, dir=input_dir)
    progress_bar.update(1)


@click.command()
def batch_migrate():
    """Migrate collections in batches."""

    input_dirs = [
        "/home/tadeas/mongo2arango/data_mongo/batches/split-1",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-2",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-3",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-4",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-5",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-6",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-7",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-8",
        "/home/tadeas/mongo2arango/data_mongo/batches/split-9",
        "/home/tadeas/mongo2arango/data_mongo-4K/split-1",
        "/home/tadeas/mongo2arango/data_mongo-4K/split-2",
        "/home/tadeas/mongo2arango/data_mongo-4K/split-3",
        "/home/tadeas/mongo2arango/data_mongo-4K/split-4",
    ]

    # Create a list to hold the process instances
    processes = []
    progress_bars = []

    # Start the function concurrently for each input directory
    for input_dir in input_dirs:
        progress_bar = tqdm(total=1, desc=f"Importing from {input_dir}")
        process = multiprocessing.Process(
            target=migrate_single_directory, args=(input_dir, progress_bar)
        )
        processes.append(process)
        progress_bars.append(progress_bar)
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()

    # Close the progress bars
    for progress_bar in progress_bars:
        progress_bar.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  # Set logging level to INFO
    batch_migrate()
