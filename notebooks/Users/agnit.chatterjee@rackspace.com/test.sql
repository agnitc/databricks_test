-- Databricks notebook source
-- MAGIC %md
-- MAGIC <h1>TESTING OUT GIT NOTEBOOK FEATURES

-- COMMAND ----------

create or replace temp view temp_view(dummy) as values("abcd")

-- COMMAND ----------

create or replace temp view temp_view(dummy) as select * from  values("abcd")

-- COMMAND ----------

create or replace global temp view temp_view(dummy) as values("abcd")

-- COMMAND ----------

create or replace global temp view temp_view(dummy) as select * from values("abcd")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/test_table", True)

-- COMMAND ----------

create table test_table(dummy string);

-- COMMAND ----------

insert into test_table values("dummy")

-- COMMAND ----------

create or replace table test_table2 as select * from values("abcd")

-- COMMAND ----------

create or replace table test_table2(dummy) as select * from values("abcd")

-- COMMAND ----------

create table test_table2
(
dummy string
)
as 
select * from values("abcd")

-- COMMAND ----------

select * from test_table

-- COMMAND ----------

create view test_view as select * from values("abcd")

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table = "test_table2"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select * from ${table}

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC table_name = "test_table2"
-- MAGIC schema_name = "default"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f'{schema_name}.{table_name}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("app.table_name", table_name)
-- MAGIC spark.conf.set("app.schema_name", schema_name)

-- COMMAND ----------

select col1 from ${app.schema_name}.${app.table_name}

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM ____________________

-- COMMAND ----------

show databases

-- COMMAND ----------

create or replace table sales as 
select 1 as batchId ,
	from_json('[{ "employeeId":1234,"sales" : 10000 },{ "employeeId":3232,"sales" : 30000 }]',
         'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
  current_timestamp() as insertDate
union all 
select 2 as batchId ,
  from_json('[{ "employeeId":1235,"sales" : 10500 },{ "employeeId":3233,"sales" : 32000 }]',
	                'ARRAY<STRUCT<employeeId: BIGINT, sales: INT>>') as performance,
                current_timestamp() as insertDate


-- COMMAND ----------

select * from sales

-- COMMAND ----------

select performance.sales from sales