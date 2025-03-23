# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ce14881-6c32-4a76-9244-eaee87b2cd74",
# META       "default_lakehouse_name": "lh_vegas_caching_bug",
# META       "default_lakehouse_workspace_id": "3306b568-71e0-4a66-982b-aec0b6ce229a"
# META     },
# META     "environment": {}
# META   }
# META }

# PARAMETERS CELL ********************

vegas_caching = True
delete_data_after_testing = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set('spark.synapse.vegas.useCache', vegas_caching)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Reproduce Fabric Vegas caching duplicate data bug
# 
# This notebook create test csv files into a lakehouse, reads those files with and without 'vegas' caching to check if the row count matches between written amount, total row count of individual parts, and when the folder containing the files is read as a whole.
# 
# Test data includes n amount of rows with one row removed (row_id == 2) and additional 10 rows added, so total row count written is n + 9. 
# 
# Row_id column is unique and should contain only numeric values. 
# 
# CSV file has a header and it uses ';' as a separator.
# 
# Feb 2025, the bug reproduces every time using default Fabric settings (Fabric runtime 1.3, medium executor and driver) when Vegas caching is enabled.

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, expr, asc
from uuid import uuid4

# uuid to enable concurrent runs without clashing
test_data_path = f"Files/bug_hunt/data/{uuid4()}"

def create_dummy_data(rows : int, row_id_starting : int) -> DataFrame:
    df = (spark
        .range(rows)
        .select(
            (col('id') + row_id_starting).alias('row_id'),
            expr('uuid()').alias('basic_data'), # just to beef it up
            expr("uuid()").alias('more_data'),
            expr("uuid()").alias('more_data_2'),
            expr("uuid()").alias('more_data_3'),
            expr("uuid()").alias('more_data_4'),
            expr("uuid()").alias('more_data_5'),
            expr("uuid()").alias('more_data_6')
        )
    )
    
    return df

def generate_and_save_dummy_data(row_count: int, path: str) -> int:
    df = create_dummy_data(row_count, 1)

    # doesn't reproduce with plain data, so do some changes to data

    # update a row
    updated_df = df.withColumn('basic_data', when(df.row_id == 1, expr('uuid()')).otherwise(df.basic_data))

    # ten new rows
    new_df = (create_dummy_data(10, row_count + 1)
        .union(updated_df)
    )

    # one deleted row
    new_df = new_df.where(new_df.row_id != 2)
    print(f"Writing {path}: {new_df.count()} rows")
    
    new_df.write.csv(path, sep=';', header=True)

    return new_df.count()

def verify_data(path : str) -> int:
    print(f"Verifying per part row count for {path}")
    row_count = 0

    for file in notebookutils.fs.ls(path):
        if(file.name != "_SUCCESS"):
            df_part = spark.read.csv(f"{path}/{file.name}", sep=';', header=True)
            row_count += df_part.count()
    
    print(f"Total row count of parts: {row_count}")

    df_all = spark.read.csv(path, sep=';', header=True)
    print(f"Row count when read in single df: {df_all.count()}")

    return df_all.count()

def check_duplicate_rows(test : str):
    df = spark.read.csv(f"{test_data_path}/{test}", sep=";", header=True)
    print(f"Test {test} - row count: {df.count()}")
    print('Duplicates')
    (df.groupBy('row_id')
        .count()
        .orderBy(col('count').desc())
        .where("count > 1")
        .show()
    )
    print('Row header in data')
    (df.where('row_id == "row_id"').show())

def run_test(rows : int, name : str, generate_new_data : bool = True):
    path = f"{test_data_path}/{name}"
    target_row_count = 0
    
    if (generate_and_save_dummy_data(rows, path) == verify_data(path)):
        print(f"Test {name} PASSED")
    else:
        print(f"Test {name} FAILED")
        check_duplicate_rows(name)

def print_filenames_and_sizes(test : str):
    files = (notebookutils.fs.ls(f"{test_data_path}/{test}"))
    print(f"** Files in {test_data_path}/{test} **")
    for file in files:
        print(f"{file.name} : {file.size}")

def delete_all_test_data():
    try:
        notebookutils.fs.rm(test_data_path, True)
    except:
        None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configure and run the tests
# 
# Test fails is written row count is different than read row count.
# 
# All tests should pass.
# 
# Test has passed when there is PASSED test in the log, and failed when there is a FAILED text.
# Example:
# * Test 20m PASSED
# * Test 20m FAILED

# CELL ********************


# Just is case
delete_all_test_data()

# Test configuration

tests = [
    {"name": '100k', 'row_count':     100000},
    {"name": '1m', 'row_count':      1000000},
    {'name': '5m', 'row_count':      5000000},
    {'name': '10m', 'row_count':    10000000},
    {'name': '10.01m', 'row_count': 10010000},
    {'name': '20m', 'row_count':    20000000},
    {'name': '30m', 'row_count':    30000000},
]




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for test in tests:
    run_test(test['row_count'], test['name'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Print filenames and sizes (Optional step)

# CELL ********************

# This is not actually needed but can be commented of

for test in tests:
    print_filenames_and_sizes(test['name'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Delete the data (if needed)

# CELL ********************

if (delete_data_after_testing):
    delete_all_test_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
