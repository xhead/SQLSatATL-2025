# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7a28a711-5d98-4168-8253-1134df09fc00",
# META       "default_lakehouse_name": "DemoLakehouse",
# META       "default_lakehouse_workspace_id": "18334567-bddc-46f6-853c-5b5e7e882405"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import *
from pyspark.sql.functions import *
from notebookutils import mssparkutils
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # MergeByDateRange
# Copies rows from [staging_table] to [target_table], replacing all rows in [target_table] within the min/max range of the [date_range_replace_column] from the staging table. 
# 
# It is assumed that the [staging_table] contains a complete set of rows for the time period specified in the min/max values of the [date_range_replace_column].

# CELL ********************

def MergeByDateRange(staging_table,target_table, date_range_replace_column):

    target_df = spark.sql(f"select * from {target_table}")
    source_df = spark.sql(f"select * from {staging_table}")
    
    #check if table has any columns or has already columns but they are empty
    if len(target_df.columns)==0 or target_df.isEmpty():    
        # first time insert when nothing to delete    
        (source_df
        .write
        .format("delta")    
        .mode("overwrite")    
        .option("mergeSchema", "true")    
        .saveAsTable(target_table)    
        )
    else:     
        # insert with replacement by 'date_range_replace_column'    
        min_date = source_df.agg(min(col(date_range_replace_column))).collect()[0][0]
        max_date = source_df.agg(max(col(date_range_replace_column))).collect()[0][0]
        replace_where = f"`{date_range_replace_column}` between '{min_date}' and '{max_date}'"    
        (source_df    
        .write    
        .format("delta")    
        .mode("overwrite")    
        .option("replaceWhere", replace_where)    
        .option("mergeSchema", "true")    
        .saveAsTable(target_table)    
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # MergeUpsert
# Merge [staging_table] into [target_table], matching on [key_column]
# 
# Will do inserts of new rows and updates of matching rows

# CELL ********************

def MergeUpsert(staging_table,target_table, key_column):

    #read from delta tables to spark dataframes
    target_df = spark.sql(f"select * from {target_table}")
    source_df = spark.sql(f"select * from {staging_table}")
    
    #check if table has any columns or has already columns but they are empty
    if len(target_df.columns)==0 or target_df.isEmpty():
        # first time insert when nothing to delete
        (source_df
        .write
        .format("delta")    
        .mode("overwrite")    
        .option("mergeSchema", "true")    
        .saveAsTable(target_table)    
        )
    else:
        #read DeltaTable from Lakehouse path
        lakehouse_table_path = f"{get_lakehouse_path()}/Tables/{target_table.replace('.','/')}"
        target_dt = DeltaTable.forPath(spark, lakehouse_table_path)

        # Alias the columns to avoid name conflicts
        target_dt = target_dt.alias("target")
        source_df = source_df.alias("source")
        
        (target_dt
            .merge(source_df, f"source.`{key_column}` = target.`{key_column}`")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            # .whenNotMatchedBySourceDelete()
            .withSchemaEvolution()
            .execute()
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # OverwriteTable
# The rows in the [staging_table] replace (overwrite) all rows in the [target_table]

# CELL ********************

def OverwriteTable(staging_table,target_table):

    source_df = spark.sql(f"select * from {staging_table}")
    
    (source_df
    .write
    .format("delta")    
    .mode("overwrite")    
    .option("mergeSchema", "true")    
    .saveAsTable(target_table)    
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # get_lakehouse_path
# Returns the ABFSS:// path of the default lakehouse.
# 
# This is used by the MergeUpsert function.

# CELL ********************

def get_lakehouse_path():
    for mp in mssparkutils.fs.mounts():
        if mp.mountPoint == '/default':
            lakehouse_path = mp.source
    return lakehouse_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
