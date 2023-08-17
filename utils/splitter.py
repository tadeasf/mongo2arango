import ijson
import simplejson as json


def split_json(input_file, output_pattern, chunk_size):
    """
    Splits a large JSON file into smaller chunks.

    :param input_file: Path to the input JSON file.
    :param output_pattern: Pattern for the output files, e.g., 'chunk_{}.json'.
    :param chunk_size: Number of records per chunk.
    """

    with open(input_file, "r") as f:
        items = ijson.items(f, "item")
        current_chunk = []
        chunk_number = 0

        for item in items:
            current_chunk.append(item)
            if len(current_chunk) == chunk_size:
                with open(output_pattern.format(chunk_number), "w") as out_f:
                    json.dump(current_chunk, out_f)
                chunk_number += 1
                current_chunk = []

        # Handle the last chunk
        if current_chunk:
            with open(output_pattern.format(chunk_number), "w") as out_f:
                json.dump(current_chunk, out_f)


if __name__ == "__main__":
    split_json(
        "./../data_mongo/assignations.json",
        "./../data_mongo/ass/assignations_{}.json",
        1,
    )

# Example usage:
# split_json('large_file.json', 'chunk_{}.json', 10000)
