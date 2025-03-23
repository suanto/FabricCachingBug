# Description
Notebooks and a Lakehouse to reproduce a bug in Fabric's [Intelligent (Vegas) caching](https://learn.microsoft.com/en-us/fabric/data-engineering/intelligent-cache).

# How it works
* Run nb_reproduce_vegas_caching_bug-notebook with caching enabled or disabled (vegas_caching = True or False) OR
* Run nb_reproduce_vegas_caching_bug_-_cache_enabled/disabled notebook. These notebooks can also be used to schedule the runs to test the effect of enabling/disabling the cache on runtimes.

## nb_reproduct_vegas_caching_bug
* The notebook creates a dummy dataset, adds 9 rows to it, saves it as CSV-file, reads it back, and compares the row count.
    * So if a test is configured to write 10.000 rows, it add 9 rows to it and actually writes 10.009 rows. Reading should return the equal amount of rows.
* The row amounts used can be configured
* If the test has passed (written rows == read rows), the log contains the following line:
    * "Test <TEST NAME> PASSED"
* If the test has failed (written rows != read rows), the log contains the following line:
    * "Test <TEST NAME> FAILED"

# Install
* Sync the repo to a Fabric Workspace

# The bug

Reading a large (5m+) CSV-file from OneLake adds phantom rows to the data. It doesn't happen every time, but so often, that the bug can be reproduced every run of the notebook. The added rows are the headers of the CSV-file and they seem to be related to the amount of executor core in use.

Steps to reproduce:
1. Write a large CSV file (5m+ rows) to OneLake
2. Read it back
3. Expected result: The read file should contain equal amount of rows than the file written
4: Actual result: The file contains more rows the written. Usually 1-17 rows more. All extra rows are duplicates and contain the CSV-file header.

![](/assets/error_log_example.png)