import pandas as pd
import heapq
import time
from multiprocessing import Pool
import csv
from utilis import calc_num_chunks, write_execution_time, merge_files






def merge_chunks(in_files, output_file, key_pos, header):
    """
    Merged sorted chunks in parallel.

    :param chunks: List of sorted dataframes
    :param key: The key on which the sort should be done
    :return: Merged and sorted dataframe
    """

    # There are still chunks to be merged
    while len(in_files) > 2:
        # Set up multiprocessing Pool
        with Pool(processes=len(in_files)//2) as pool:
            # Each processor will merge two chunks together
            tasks = [(in_files[i]+in_files[i+1], [in_files[i], in_files[i+1]],2, key_pos, header) for i in range(0,len(in_files)-1,2)]
            in_files_new = pool.starmap(merge_files_by_name, tasks)

            # If the number of chunks is odd, add the not merged chunk to the next round
            if len(in_files) % 2 == 1:
                in_files_new.append(in_files[-1])

            in_files = in_files_new

    merge_files_by_name(output_file, in_files, 2, key_pos, header)



def merge_files_by_name(output_file, in_files_names, k, key_pos, header):
    """
    Merge K files into one file

    :param output_file: The sorted output file
    :param in_files_names: List of input files names
    :param k: The number of files to merge
    :param key_pos: The key positin on which the sort should be done
    :param header: File header
    :return: output file name
    """
    harr = []
    out = open(output_file, "w")
    out.write(header)
    # Open output files in read mode.
    in_files = [open(file, 'r') for file in in_files_names]
    in_files2 = [open(file, 'r') for file in in_files_names]
    in_files_csv = [csv.reader(in_files2[i]) for i in range(k)]
    # Create a min heap with k heap nodes.
    # Every heap node has first element of scratch output file
    for i in range(k):
        headings = next(in_files[i])
        headings = next(in_files_csv[i])
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
        in_files2[i].close()
    out.close()

    return output_file



def read_sort_chunk(file_name, output_file, start_row, chunk_size, key):
    """
     Function to read and sort a specific chunk of the CSV file

    :param file_name: The name of the CSV file
    :param start_row: The row to start reading from
    :param chunk_size: The number of rows to read
    :param key: The key on which the sort should be done
    :return: Sorted dataframe with the specified chunk of data
    """
    chunk = pd.read_csv(file_name, skiprows=range(1, start_row), nrows=chunk_size)
    chunk = chunk.sort_values(key)
    chunk.to_csv(output_file, index=False)



def parallel_create_initial_runs(input_file, key, chunk_size, num_chunks):
    """
    Using a merge-sort algorithm, create the
    initial runs and divide them evenly among the output files

    :param input_file:
    :param key: The key on which the sort should be done
    :param chunk_size: Number of chunks needed to process all data
    :param num_chunks: Number of rows to process in parallel
    """

    # Set up multiprocessing Pool
    with Pool(processes=num_chunks) as pool:
        # Create a list of arguments (file_name, output_file, start_row, chunk_size, key)
        tasks = [(input_file, str(i), i * chunk_size + 1, chunk_size, key) for i in range(num_chunks)]

        # Read and sort the chunks in parallel
        pool.starmap(read_sort_chunk, tasks)




def parallel_external_sort(input_file, output_file, key, num_chunks, chunk_size):
    """
    For sorting data stored on disk in parallel.

    :param input_file:
    :param output_file:
    :param key: The key on which the sort should be done
    :param num_chunks: Number of chunks needed to process all data
    :param chunk_size: Number of rows to process in parallel
    """

    # read the input file, create the initial runs,
    # and assign the runs to the scratch output files
    parallel_create_initial_runs(input_file, key, chunk_size, num_chunks)

    in_file = open(input_file, "r")
    header = next(in_file)
    headers = header.strip().split(",")
    key_pos = headers.index(key)

    # Merge the runs using the K-way merging
    merge_files(output_file, num_chunks, key_pos, header)
    # merge_chunks([str(i) for i in range(num_chunks)], output_file, key_pos, header) # parallel merge



def main():

    # Save the start time of the execution
    start = time.time()

    input_file = 'sample.csv'
    output_file = 'sorting-step3.csv'
    key = 'data'

    # The size of each partition
    chunk_size = 2000
    num_chunks = calc_num_chunks(input_file, chunk_size)

    parallel_external_sort(input_file, output_file, key, num_chunks, chunk_size)

    # Save the end time of the execution
    end = time.time()
    write_execution_time('sorting-step3_process_time.txt', start, end)


if __name__ == "__main__":
    main()