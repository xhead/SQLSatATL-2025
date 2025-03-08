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

# MAGIC %%sql
# MAGIC 
# MAGIC select year(OrderDate), min(SalesOrderID), max(SalesOrderID), count(*)
# MAGIC from dw.Sales 
# MAGIC group by all;
# MAGIC 
# MAGIC select year(s.OrderDate), min(d.SalesOrderID), max(d.SalesOrderID), 
# MAGIC     count(*)
# MAGIC from dw.SalesDetail d
# MAGIC     left join dw.Sales s on d.SalesOrderID = s.SalesOrderID
# MAGIC group by all;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from dw.Sales 
# MAGIC where year(OrderDate) < 2025;
# MAGIC 
# MAGIC delete from dw.SalesDetail
# MAGIC -- where SalesOrderID < 71774367;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
