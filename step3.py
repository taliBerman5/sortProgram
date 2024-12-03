import pandas as pd
import time
from multiprocessing import Pool
from utilis import merge_sort, write_execution_time
import math


def read_sort_chunk(file_name, start_row, chunk_size, key):
    """
     Function to read and sort a specific chunk of the CSV file

    :param file_name: The name of the CSV file
    :param start_row: The row to start reading from
    :param chunk_size: The number of rows to read
    :param key: The key on which the sort should be done
    :return: Sorted dataframe with the specified chunk of data
    """
    chunk = pd.read_csv(file_name, skiprows=range(1, start_row), nrows=chunk_size)
    return chunk.sort_values(key)


def process_csv_chunks(file_name, chunk_size, key):
    """
    Parallelize CSV reading by processing chunks of data in parallel

    :param file_name: The name of the CSV file
    :param chunk_size: Number of rows to process in parallel
    :param key: The key on which the sort should be done
    :return: List of sorted dataframes, each of size chunk_size
    """
    # Get the total number of rows in the file
    total_rows = sum(1 for line in open(file_name)) - 1  # Subtracting header row

    num_chunks = math.ceil(total_rows / chunk_size)

    # Set up multiprocessing Pool
    with Pool(processes=num_chunks) as pool:
        # Create a list of arguments (file_name, start_row, chunk_size, key)
        tasks = [(file_name, i * chunk_size + 1, chunk_size, key) for i in range(num_chunks)]

        # Read and sort the chunks in parallel
        results = pool.starmap(read_sort_chunk, tasks)

    return results


def merge_chunks(chunks, key):
    """
    Merged sorted chunks in parallel.

    :param chunks: List of sorted dataframes
    :param key: The key on which the sort should be done
    :return: Merged and sorted dataframe
    """

    # There are still chunks to be merged
    while len(chunks) > 1:
        # Set up multiprocessing Pool
        with Pool(processes=len(chunks)//2) as pool:
            # Each processor will merge two chunks together
            tasks = [(chunks[i], chunks[i+1], key) for i in range(0,len(chunks)-1,2)]
            new_chunks = pool.starmap(merge_sort, tasks)

            # If the number of chunks is odd, add the not merged chunk to the next round
            if len(chunks) % 2 == 1:
                new_chunks.append(chunks[-1])

            chunks = new_chunks

    return chunks[0]

def parallel_sort_by_chunk(file_name, chunk_size, key):
    """
    Sort file data according to the key field.
    Allowing processing only chunk_size records at a time in each server

    :param file_name: The name of the CSV file
    :param chunk_size: Number of rows to process in parallel
    :param key: The key on which the sort should be done
    :return: Sorted dataframe
    """
    chunks = process_csv_chunks(file_name, chunk_size, key)
    return merge_chunks(chunks, key)


if __name__ == "__main__":
    # Save the start time of the execution
    start = time.time()

    df = parallel_sort_by_chunk('sample.csv', chunk_size=2000, key='data')

    # save the result to a new csv file
    df.to_csv('sorting-step3.csv', index=False)

    # Save the end time of the execution
    end = time.time()
    write_execution_time('sorting-step3_process_time.txt', start, end)