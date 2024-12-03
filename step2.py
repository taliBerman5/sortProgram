import heapq
import time
import csv
from utilis import calc_num_chunks, write_execution_time, merge_files
import pandas as pd





def create_initial_runs(input_file, key, chunk_size, num_chunks):
    """
    Using a merge-sort algorithm, create the
    initial runs and divide them evenly among the output files

    :param input_file:
    :param key: The key on which the sort should be done
    :param chunk_size: Number of chunks needed to process all data
    :param num_chunks: Number of rows to process in parallel
    """
    in_file = open(input_file, "r")

    # output scratch files
    out_files = [open(str(i), 'w') for i in range(num_chunks)]

    next_output_file = 0

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Sort each chunk
        chunk = chunk.sort_values(key)

        # write the records to the appropriate scratch output file
        chunk.to_csv(str(next_output_file), index=False)

        next_output_file += 1

    # close input and output files
    for i in range(num_chunks):
        out_files[i].close()

    in_file.close()


def external_sort(input_file, output_file, key, num_chunks, chunk_size):
    """
    For sorting data stored on disk

    :param input_file:
    :param output_file:
    :param key: The key on which the sort should be done
    :param num_chunks: Number of chunks needed to process all data
    :param chunk_size: Number of rows to process in parallel
    """
    # Read the input file, create the initial runs,
    # and assign the runs to the scratch output files
    create_initial_runs(input_file, key, chunk_size, num_chunks)

    # Get the key position from the header
    in_file = open(input_file, "r")
    header = next(in_file)
    headers = header.strip().split(",")
    key_pos = headers.index(key)

    # Merge the runs using the K-way merging
    merge_files(output_file, num_chunks, key_pos, header)


def main():
    # Save the start time of the execution
    start = time.time()

    input_file = 'sample.csv'
    output_file = 'sorting-step2.csv'
    key = 'data'

    # The size of each partition
    chunk_size = 2000
    num_chunks = calc_num_chunks(input_file, chunk_size)

    external_sort(input_file, output_file, key, num_chunks, chunk_size)

    # Save the end time of the execution
    end = time.time()
    write_execution_time('sorting-step2_process_time.txt', start, end)


if __name__ == "__main__":
    main()