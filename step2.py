import heapq
import time
import csv

from utilis import calc_num_chunks, write_execution_time
import pandas as pd


def merge_files(output_file, k, key_pos, header):
    """
    Merge K files into one file

    :param output_file: The sorted output file
    :param k: The number of files to merge
    :param key_pos: The key positin on which the sort should be done
    :param header: File header
    """
    harr = []
    out = open(output_file, "w")
    out.write(header)

    # Open output files in read mode.
    in_files = [open(str(i), 'r') for i in range(k)]
    in_files2 = [open(str(i), 'r') for i in range(k)]
    in_files_csv = [csv.reader(in_files2[i]) for i in range(k)]

    # Create a min heap with k heap nodes.
    # Every heap node has first element of scratch output file
    for i in range(k):
        header = next(in_files[i])   # pass the header
        header = next(in_files_csv[i])   # pass the header
        element = in_files[i].readline().strip()
        if element:
            key_val = next(in_files_csv[i])[key_pos]
            heapq.heappush(harr, (key_val, element, i))

    count = 0
    while count < k:
        # Get the minimum element and store it in output file
        root = heapq.heappop(harr)
        out.write(root[1] + '\n')

        # Find the next element that will
        # replace current root of heap.
        element = in_files[root[2]].readline().strip()
        if element:
            key_val = next(in_files_csv[root[2]])[key_pos]
            heapq.heappush(harr, (key_val, element, root[2]))
        else:
            count += 1

    # close input and output files
    for i in range(k):
        in_files[i].close()
    out.close()


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