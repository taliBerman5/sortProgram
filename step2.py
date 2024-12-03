import pandas as pd
import time
from utilis import merge_sort, write_execution_time



def sort_by_chunks(file_name, chunk_size, key):
    """
    Sort the file name allowing processing only chunk_size records at a time

    :param file_name: The name of the CSV file
    :param chunk_size: Number of rows to process in parallel
    :param key: The key on which the sort should be done
    :return: Sorted dataframe
    """
    #Initiate an empty dataframe
    df = pd.DataFrame()

    # Load a CSV file in chunks of chunk_size records
    for chunk in pd.read_csv(file_name, chunksize=chunk_size):
        chunk = chunk.sort_values(key)
        df = merge_sort(df, chunk, key)

    return df



if __name__ == "__main__":
    # Save the start time of the execution
    start = time.time()

    df = sort_by_chunks('sample.csv', chunk_size=2000, key='data')

    # Save the result to a new csv file
    df.to_csv('sorting-step2.csv', index=False)

    # Save the end time of the execution
    end = time.time()
    write_execution_time('sorting-step2_process_time.txt', start, end)


