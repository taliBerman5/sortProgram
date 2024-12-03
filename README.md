# Sort CSV File

There are three algorithms to sort data from sample.csv file based on the `data` field
* step1: Sort without limitations
* step2: Can only Process up to 2000 records at a time
* step3: Same limitation as in step2 but there are several servers to speed-up time

The three algorithms creates an output file `sorting-step{n}` with the sorted data. 
In addition, `sorting-step{n}_process_time` file is created that holds the execution time for each algorithm. 

## Installations 
On Ubuntu
```bash
sudo apt update
sudo apt install python3 python3-pip -y
pip3 install pandas
```
On Windows
```bash
Download python from the official website https://www.python.org/
pip3 install pandas
```

## Quick Start

Run example of step1 using the terminal:

```bash
$ cd sortProgram
$ python step1
```