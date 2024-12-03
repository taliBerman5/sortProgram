import pandas as pd


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


def write_execution_time(name, start, end):
    """
    Write execution time to file names name

    :param name: File name
    :param start: Start time of execution
    :param end: End time of execution
    """
    with open(name, 'w') as file:
        file.write(str("%.5f" % (end - start)))


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
