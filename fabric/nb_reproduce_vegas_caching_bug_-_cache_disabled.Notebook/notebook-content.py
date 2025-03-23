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

# MARKDOWN ********************

# ## Run the notebook with caching disabled

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "conf":
# MAGIC     {
# MAGIC         "spark.synapse.vegas.useCache": "false"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.run('nb_reproduce_vegas_caching_bug', 
    2000, # timeout, just some long one
    {'vegas_caching': False,
    'delete_data_after_testing': True}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
