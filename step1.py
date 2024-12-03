import pandas as pd
import time
from utilis import write_execution_time


# Save the start time of the execution
start = time.time()

# Read the CSV file to a dataframe
df = pd.read_csv('sample.csv')

# Sort the data frame according to data field
df = df.sort_values('data')

# Save the result to a new csv file
df.to_csv('sorting-step1.csv', index=False)

# Save the end time of the execution
end = time.time()

# Write the execution time to a file
write_execution_time('sorting-step1_process_time.txt', start, end)
