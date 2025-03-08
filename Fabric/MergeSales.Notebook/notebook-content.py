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

%run CommonLakehouseMerge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MergeByDateRange("staging.Sales", "dw.Sales", "OrderDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
