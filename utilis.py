import pandas as pd
import math
import csv
import heapq


def merge_sort(df1, df2, key):
    """
    Merge two sorted dataframes df1 and df2 according the key

    :param df1: Sorted DataFrame
    :param df2: Sorted DataFrame
    :param key: The key on which the sort should be done
    :return: Merged and sorted DataFrame
    """
    new_rows = []
    i1 = 0
    i2 = 0

    while i1 < len(df1) and i2 < len(df2):
        if df1.iloc[i1][key] < df2.iloc[i2][key]:
            new_rows.append(df1.iloc[i1].to_dict())
            i1 += 1
        else:
            new_rows.append(df2.iloc[i2].to_dict())
            i2 += 1

    new_rows.extend(df1.iloc[i1:].to_dict('records'))
    new_rows.extend(df2.iloc[i2:].to_dict('records'))
    return pd.DataFrame(new_rows)

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
        in_files2[i].close()
    out.close()


def write_execution_time(name, start, end):
    """
    Write execution time to file names name

    :param name: File name
    :param start: Start time of execution
    :param end: End time of execution
    """
    with open(name, 'w') as file:
        file.write(str("%.5f" % (end - start)))


def calc_num_chunks(file_name, chunk_size):
    # Get the total number of rows in the file
    total_rows = sum(1 for line in open(file_name)) - 1  # Subtracting header row
    return math.ceil(total_rows / chunk_size)


def compare_files(file1, file2):
    """
    Compares file1 and file2 data and prints the difference

    :param file1: First file name
    :param file2: Second file name
    """
    # Read CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Compare dataframes
    diff = df1.compare(df2)

    # Print the differences
    print(f"Differences between {file1} and {file2}:")
    print(diff)
