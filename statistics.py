# Databricks notebook source
cc = 'SGP'

# COMMAND ----------

ips = spark.read.table('ndaniel_db.tmp_sgp_ips')

# COMMAND ----------

try:
  del prev
except:
  ""
cc = 'LBN'
arr = [
  [f'matan_db.ssid_enrichemnt_pre_raw_{cc}', 'step 1'],
  [f'matan_db.ssid_enrichemnt_remove_outliers_{cc}', 'step 2'],
  [f'matan_db.ssid_enrichemnt_remove_false_ssid_datacenters_{cc}', 'step 3'],
  [f'matan_db.ssid_enrichemnt_remove_small_ip_{cc}', 'step 3.1'],
  [f'matan_db.ssid_enrichemnt_network_dict_{cc}', 'step 4'],
  [f'matan_db.ssid_enrichemnt_collect_networks_{cc}','step 4.1'],
  [f'matan_db.ssid_enrichemnt_time_limit_{cc}', 'step 5'],
  [f'matan_db.ssid_enrichemnt_multi_network_same_ssid_ip_{cc}', 'step 6'],
  [f'matan_db.ssid_enrichemnt_multi_clusters_same_ssid_ip_{cc}', 'step 6.1']
]

for a in arr:
  num = spark.read.table(a[0]).select('ip').drop_duplicates().count()
  
  print(f'''
Step: {a[1]}
IPs:{num:,}
Table: {a[0]}''')
  try:
    print(f'Funnel: {(num/prev):%}')
  except:
    ''
  prev=num
  print('-'*100)

# COMMAND ----------

ips_count = ips.select('ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_pre_raw_{cc}'
step_m_1_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_time_limit_{cc}'
step_2_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_remove_multi_ssids_per_ip_{cc}'
step_0_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_remove_false_ssid_datacenters_{cc}'
step_1_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()


table_name = f'matan_db.ssid_enrichemnt_remove_false_ssid_datacenters_{cc}'
step_3_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()


table_name = f'matan_db.ssid_enrichemnt_network_dict_{cc}'
step_4_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()


table_name = f'matan_db.ssid_enrichemnt_post_clustering_{cc}'
step_5_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_final_{cc}'
step_6_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()

table_name = f'matan_db.ssid_enrichemnt_final_total_relavent_ips_{cc}'
step_7_ips_c = spark.read.table(table_name).select('ip').join(ips,'ip').drop_duplicates().count()


# COMMAND ----------

cc = ['MYS','LBN','SGP','PSE','TUR','IDN']
for c in cc:
  df = spark.read.table('bids_core').where(f'pday = 20221025 and country_code = "{c}"').select('ip')
  ips_enriched = spark.read.table(f'matan_db.ssid_enrichemnt_multi_clusters_same_ssid_ip_{c}').select('ip').where('latitude is not null').drop_duplicates()
  ips_enriched_with_matan = spark.read.table(f'matan_db.ssid_enrichemnt_final_total_relavent_ips_{c}').select('ip').where('latitude is not null').drop_duplicates()

  
  total_events = df.count()
  enriched_events = df.join(ips_enriched,'ip').count()
  enriched_events_matan = df.join(ips_enriched_with_matan,'ip').count()
  
  print(f'''
Country - {c}
Total Events - {total_events:,}
Enriched Events - {enriched_events:,}
  
Ratio - {enriched_events/total_events}

Enriched Events After Matan's addtion - {enriched_events_matan:,}

Ratio - {enriched_events_matan/total_events}

  ''')
  
  print("*"*50)
  
  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_lbn
# MAGIC where network_id like 'LBN_Tenda_%'
# MAGIC -- order by ifas desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_lbn
# MAGIC order by ifas desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_sgp
# MAGIC where ssid like 'FourS%'

# COMMAND ----------

from geopy.distance import geodesic
from pyspark.sql.types import StringType, LongType, DoubleType
sqlContext.udf.register("distance", lambda lat1,lon1,lat2,lon2: geodesic((lat1,lon1),(lat2,lon2)).meters, DoubleType())

# COMMAND ----------

from pyspark.sql.functions import col,lit, countDistinct,percent_rank,rand, desc, explode, split, approx_count_distinct, regexp_replace, lower, max as max_spark, when, concat,count, translate, min as min_spark, max as max_spark
from pyspark.sql.window import Window
from sklearn.cluster import OPTICS
import numpy as np
import pandas as pd

# COMMAND ----------

for cc in ['SGP','MYS','LBN','PSE','TUR','IDN']:
  ### step 4

  table_name = f'matan_db.ssid_enrichemnt_final_{cc}'
  sdf = spark.read.table(table_name)
  relavent_ssid_ip = spark.read.table(f'matan_db.ssid_enrichemnt_pre_raw_{cc}').selectExpr('ip as ip_enrch','ssid','latitude as latitude_enrch','longitude as longitude_enrch').drop_duplicates()
  relavent_ssid_ip = sdf.join(relavent_ssid_ip,'ssid')
  relavent_ssid_ip = relavent_ssid_ip.selectExpr('ip_enrch as ip','ssid','network_id','latitude','longitude','distance(latitude,longitude,latitude_enrch,longitude_enrch) distance')
  relavent_ssid_ip = relavent_ssid_ip.where('distance<=500')
  relavent_ssid_ip = relavent_ssid_ip.groupBy('ip','ssid','network_id','latitude','longitude').agg(min_spark('distance').alias('min_distance'),max_spark('distance').alias('max_distance'), count(lit(1)).alias('events'))
  ### step 5

  table_name = f'matan_db.ssid_enrichemnt_final_total_relavent_ips_{cc}'
  relavent_ssid_ip.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)

# COMMAND ----------


