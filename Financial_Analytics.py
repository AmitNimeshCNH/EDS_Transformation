# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

prd=spark.sql('select p.business as cod_business,p.Product_Category as des_product_category,p.Product_Line as cod_product_line, p.Family as des_family, p.Model as cod_model, p.Brand_Code as cod_brand, p.Plant_Code as cod_plant, p.Plant as des_plant, p.Brand as des_brand,p.segment as des_segment,p.pk as id_pk, g.ISO as ISO,g.ISO_country as ISO_country,null as geo_id, g.region_AG as region_AG,g.region_CE as region_CE,g.Region_AG_sort_order as Region_AG_sort_order,g.Region_CE_sort_order, g.Sub_region_AG_sort_order as Sub_region_AG_sort_order, g.Sub_region_CE_sort_order as Sub_region_CE_sort_order,g.CNHi_Region as cod_region,g.Sub_region_AG as Sub_region_AG,g.Sub_region_CE as Sub_region_CE,g.Area_AG as Area_AG,g.Area_CE as Area_CE, g.Market_code as cod_market,g.Market_Desc as des_market, g.hfm_Market as cod_hfm_market, c.dtyp as cod_dtyp,substring(c.month,5,2) as dat_month,substring(c.month,1,4) as dat_year,c.value as qty_kpi_sop, c.cycle as cod_cycle from scan.tbl_odm_qlik_product_off_highway p inner join scan.vdata_odm_current_off_highway c on p.pk=c.pk inner join scan.tbl_odm_qlik_geo_off_highway g on g.ISO=c.ISO where c.dtyp in ("COIN","WHOL","PROD")')

# COMMAND ----------

prd=prd.withColumn('des_sub_region', when((col('cod_business') == 'AG'), col('sub_region_AG'))\
               .when((prd.cod_business == 'CE'), col('sub_region_CE')).otherwise(lit("")))\
       .withColumn('des_area', when((col('cod_business') == 'AG'), col('Area_AG'))\
               .when((prd.cod_business == 'CE'), col('Area_CE')).otherwise(lit("")))\
       .drop('sub_region_AG','sub_region_CE','Area_AG','Area_CE')

# COMMAND ----------

fact=prd.select('cod_business','des_product_category','cod_region','des_sub_region','des_area','cod_product_line','des_family','cod_model','cod_brand','cod_plant','des_plant','des_brand','des_segment','cod_market','des_market','cod_hfm_market','id_pk','cod_dtyp','dat_month','dat_year','qty_kpi_sop','cod_cycle')

# COMMAND ----------

fact=fact.distinct()
# fact.display() count-9590162, distinct - 9590035

# COMMAND ----------

fact.createOrReplaceTempView("view_prd_fact_test")
# fact.write.mode('overWrite').format('delta').saveAsTable('company_inventory.fact_sop_test')

# COMMAND ----------

spark.sql('select * from view_prd_fact_test').show()
# spark.sql('select * from company_inventory.fact_sop_test').show()

# COMMAND ----------

spark.sql('delete from company_inventory.fact_sop where id_pk in (select id_pk from view_prd_fact_test)')

# COMMAND ----------

spark.sql("insert into company_inventory.fact_sop select * from view_prd_fact_test where id_pk not in (select id_pk from company_inventory.fact_sop)")

# COMMAND ----------

spark.sql('select * from company_inventory.fact_sop').show()

# COMMAND ----------

# fact.write.mode('append').saveAsTable('company_inventory.fact_sop')
# Prod_Sop.write.mode('overWrite').saveAsTable('company_inventory.dim_prod_sop')
# spark.sql('drop table if exists company_inventory.fact_sop').show()
# spark.sql('desc view_prd_fact').show()
