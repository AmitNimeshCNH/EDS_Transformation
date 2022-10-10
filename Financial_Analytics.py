# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

prd=spark.sql('select p.business as cod_business,p.Product_Category as des_product_category,p.Product_Line as cod_product_line, p.Family as des_family, p.Model as cod_model, p.Brand_Code as cod_brand, p.Plant_Code as cod_plant, p.Plant as des_plant, p.Brand as des_brand,p.segment as des_segment,p.pk as id_pk, g.ISO as ISO,g.ISO_country as ISO_country,null as geo_id, g.region_AG as region_AG,g.region_CE as region_CE,g.Region_AG_sort_order as Region_AG_sort_order,g.Region_CE_sort_order, g.Sub_region_AG_sort_order as Sub_region_AG_sort_order, g.Sub_region_CE_sort_order as Sub_region_CE_sort_order,g.CNHi_Region as cod_region,g.Sub_region_AG as Sub_region_AG,g.Sub_region_CE as Sub_region_CE,g.Area_AG as Area_AG,g.Area_CE as Area_CE, g.Market_code as cod_market,g.Market_Desc as des_market, g.hfm_Market as cod_hfm_market, c.dtyp as cod_dtyp,substring(c.month,5,2) as dat_month,substring(c.month,1,4) as dat_year,c.value as qty_kpi_sop, c.cycle as cod_cycle from scan.tbl_odm_qlik_product_off_highway p inner join scan.vdata_odm_current_off_highway c on p.pk=c.pk inner join scan.tbl_odm_qlik_geo_off_highway g on g.ISO=c.ISO where c.dtyp in ("COIN","WHOL","PROD")')

# COMMAND ----------

geo=prd.select('ISO','ISO_country','cod_business','geo_id','region_AG','region_CE','Sub_region_AG','Sub_region_CE','Region_AG_sort_order','Region_CE_sort_order','Sub_region_AG_sort_order','Sub_region_CE_sort_order','Area_AG','Area_CE','des_market')

# COMMAND ----------

geo=geo.withColumn('new_region', when((col('cod_business') == 'AG'), col('region_AG'))\
               .when((geo.cod_business == 'CE'), col('region_CE')).otherwise(lit("")))\
       .withColumn('New_Subregion', when((col('cod_business') == 'AG'), col('Sub_region_AG'))\
               .when((geo.cod_business == 'CE'), col('Sub_region_CE')).otherwise(lit("")))\
       .withColumn('New_Region_Sort_Order', when((col('cod_business') == 'AG'), col('Region_AG_sort_order'))\
               .when((geo.cod_business == 'CE'), col('Region_CE_sort_order')).otherwise(lit("")))\
       .withColumn('New_Sub_Region_Sort_Order', when((col('cod_business') == 'AG'), col('Sub_region_AG_sort_order'))\
               .when((geo.cod_business == 'CE'), col('Sub_region_CE_sort_order')).otherwise(lit("")))\
       .withColumn('New_Area', when((col('cod_business') == 'AG'), col('Area_AG'))\
               .when((prd.cod_business == 'CE'), col('Area_CE')).otherwise(lit("")))

# COMMAND ----------

# display(geo)
geo=geo.withColumnRenamed('des_market', 'market')

# COMMAND ----------

geo_sop=geo.drop('sub_region_AG','sub_region_CE','Area_AG','Area_CE','region_AG','region_CE','Region_AG_sort_order','Region_CE_sort_order','Sub_region_AG','Sub_region_CE','Sub_region_AG_sort_order','Sub_region_CE_sort_order')

# COMMAND ----------

Prod_Sop=spark.sql('select brand,null as prod_id,null as product_category_id,GPL,gpl_sort_order,industry_L4, industry_L5,New_Product_Category,New_Product_Category_sort_order,plant,Product_cluster_group,Product_cluster,Product_sub_cluster,segment,Sub_Brand,industry_segment_scan_sort_order,product_cluster_sort_order,null as plateform from scan.tbl_odm_qlik_product_off_highway')

# COMMAND ----------

prd=prd.withColumn('des_sub_region', when((col('cod_business') == 'AG'), col('sub_region_AG'))\
               .when((prd.cod_business == 'CE'), col('sub_region_CE')).otherwise(lit("")))\
       .withColumn('des_area', when((col('cod_business') == 'AG'), col('Area_AG'))\
               .when((prd.cod_business == 'CE'), col('Area_CE')).otherwise(lit("")))\
       .drop('sub_region_AG','sub_region_CE','Area_AG','Area_CE')

# COMMAND ----------

prd.select('cod_business','des_product_category','cod_region','des_sub_region','des_area','cod_product_line','des_family','cod_model','cod_brand','cod_plant','des_plant','des_brand','des_segment','cod_market','des_market','cod_hfm_market','id_pk','cod_dtyp','dat_month','dat_year','qty_kpi_sop','cod_cycle').show()

# COMMAND ----------

prd.createOrReplaceTempView('view_prd_fact')

# COMMAND ----------

#spark.sql('delete from company_inventory.fact_sop where id_pk in (select id_pk from view_prd_fact)').show()
spark.sql('insert into company_inventory.fact_sop select * from view_prd_fact where id_pk not in (select id_pk from view_prd_fact)').show()

# COMMAND ----------

spark.sql('select * from view_prd_fact').show()

# COMMAND ----------

#prd.write.mode('overWrite').saveAsTable("company_inventory.fact_sop")
Prod_Sop.write.mode('overWrite').saveAsTable('company_inventory.dim_Prod_Sop')
geo_sop.write.mode('overWrite').saveAsTable("company_inventory.dim_Geo_Sop")
# spark.sql('drop table if exists company_inventory.dim_Geo_Sop').show()

# COMMAND ----------

spark.sql('select * from company_inventory.dim_Prod_Sop limit 5').show()
