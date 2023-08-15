<!-- @format -->

# mongo2arango

mongo2arango is a CLI tool designed to facilitate the migration of data from
MongoDB to ArangoDB. It provides an efficient and user-friendly way to transfer
your data between these two databases.

## Why mongo2arango?

    Seamless Migration: Easily migrate your data from MongoDB to ArangoDB without the hassle.
    Multi-threading Support: Optimize the migration process with multi-threaded data processing.
    Real-time Progress Tracking: Stay informed with a real-time progress bar during the migration.

## Prerequisites

    Ensure you have exported your MongoDB data using mongoexport. Note: Declarative types from Studio 3T export will not work with this tool. We require standard .json data.

## Why simplejson and ijson?

    simplejson: We use simplejson for its speed in encoding and decoding JSON data. It's especially beneficial when dealing with large datasets, ensuring the migration process is as fast as possible.

    ijson: ijson is utilized for its ability to parse large JSON files iteratively, without loading the entire file into memory. This means even if you have a massive MongoDB export, mongo2arango can handle it without consuming all of your system's memory.

## How to Use

Installation:

    ``` bash
    pip install mongo2arango
    ```

Setting Up ArangoDB Credentials: Before migrating data, you need to set up your
ArangoDB credentials. Run:

    ``` bash
    mongo2arango login
    ```

You will be prompted to enter your ArangoDB host, user, and password. These
credentials will be stored securely for future migrations.

Migrating Data: To migrate data, use the migrate command. You can specify
options like the target database, collection, input JSON file, and more. For
example:

    ``` bash
    mongo2arango migrate --db mydatabase --col mycollection --input /path/to/exported_data.json
    ```

If you don't specify the options, the tool will prompt you for the necessary
information.

Feel free to modify or expand upon this draft to better suit your needs.
