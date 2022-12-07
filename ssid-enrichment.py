# Databricks notebook source
from pyspark.sql.functions import col,lit, countDistinct,percent_rank,rand, desc, explode, split, approx_count_distinct, regexp_replace, lower, max as max_spark, when, concat, translate, min as min_spark, max as max_spark, count, expr, ceil as ceil_spark, sum as sum_spark, collect_set, sort_array, mean, round as round_spark, flatten, size, collect_list
from pyspark.sql.window import Window
from sklearn.cluster import OPTICS
import numpy as np
import pandas as pd

# COMMAND ----------

def dbscan_spark(sdf, key_column, epsilon, dbscan_limitation_thresh):
    sdf = sdf.withColumn('cluster_id', lit(1.1))

    def dbscan(pdf):

      from sklearn.cluster import DBSCAN

      group_locations = pdf[['latitude', 'longitude']].values
      
      if len(pdf) > dbscan_limitation_thresh:
        pdf = pdf.sample(n = dbscan_limitation_thresh)
      
      dbsc = (DBSCAN(eps=epsilon/6371., min_samples=1, algorithm='ball_tree', metric='haversine')
                .fit(np.radians(group_locations)))
      pdf['cluster_id'] = dbsc.labels_

      return pdf

    sdf = sdf.groupby(*key_column).applyInPandas(dbscan,sdf.schema)
    sdf = sdf.where(sdf.cluster_id > -1)
    return sdf

# COMMAND ----------

from geopy.distance import geodesic
from pyspark.sql.types import StringType, LongType, DoubleType
sqlContext.udf.register("distance", lambda lat1,lon1,lat2,lon2: None if max([i is None for i in [lat1,lon1,lat2,lon2] ]) else geodesic((lat1,lon1),(lat2,lon2)).meters, DoubleType())

# COMMAND ----------

try:
  cc = dbutils.widgets.get('country')
except:
  cc = 'LBN'
  
start = 2022040100
end = 2022101800

min_days_delta_first_last_seen = 15
min_number_of_ifas_per_ssid = 2

# COMMAND ----------

# Raw data
q = f"""
select ip, ssid, bssid, ifa, timestamp, latitude, longitude
from sdk_prod
where country_code = '{cc}'
      and ssid != '' and ssid != '<unknown ssid>'
      and bssid != ''
      and ptime >= {start} and ptime <= {end}
"""

# removing unwanted bssids <- notice me

raw = spark.sql(q)

# COMMAND ----------

### step 1

table_name = f'matan_db.ssid_enrichemnt_pre_raw_{cc}'
# raw.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
raw = spark.read.table(table_name)

# COMMAND ----------

# Removing ips with top 1% ssid count
threshold = 0.99

raw = raw.withColumn('ssid_per_ip',approx_count_distinct('ssid').over(Window.partitionBy('ip')))\
         .withColumn('ips_per_ssid',approx_count_distinct('ip').over(Window.partitionBy('ssid')))\
         .withColumn('percent_rank_ssid_per_ip',percent_rank().over(Window.partitionBy().orderBy(col('ssid_per_ip'))))\
         .withColumn('percent_rank_ips_per_ssid',percent_rank().over(Window.partitionBy().orderBy(col('ips_per_ssid'))))\
         .where(f'percent_rank_ssid_per_ip < {threshold} and percent_rank_ips_per_ssid < {threshold}')

# COMMAND ----------

### step 2

table_name = f'matan_db.ssid_enrichemnt_remove_outliers_{cc}'
# raw.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

#removing false ssids and datacenters
## datacenters
datacenters = spark.read.table('datacenters').withColumnRenamed('ip_string','ip').withColumn('ind',lit(True))
sdf = sdf.join(datacenters,'ip','left').where('ind is null').drop('ind','ip_decimal')

## false ssids
black_list =['BlueStacks','WiredSSID','INVALID','iPhone','Android','OPPO','Redmi','@POCO','vivo','Galaxy','HUAWEI','realme', 'NOKIA' , 'Linksys' ,
             'Family' , 'dlink' , 'SINGTEL' , 'MyRepublic' , 'TP-Link' , 'Singtel' , 'ASUS' , 'Tplink' , 'Askey' , '@unifi' , 'INVALID' , '@Maxis' , 'Dlink']
cond_bl = ''

for s in black_list:
  cond_bl = cond_bl + f'ssid not like "%{s}%" and '

cond_bl = cond_bl[:len(cond_bl) - 4]

sdf = sdf.where(cond_bl)

sdf = sdf.select('ip','bssid','ifa','timestamp','latitude','longitude')

# COMMAND ----------

### step 3

table_name = f'matan_db.ssid_enrichemnt_remove_false_ssid_datacenters_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

remove_small_ip = sdf.withColumn('latitude',round_spark('latitude',6))\
                     .withColumn('longitude',round_spark('longitude',6))\
                     .groupBy('ip').agg(countDistinct('latitude','longitude').alias('locations')).where('locations > 2')
sdf = sdf.join(remove_small_ip,'ip')

# COMMAND ----------

### step 3.1

table_name = f'matan_db.ssid_enrichemnt_remove_small_ip_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

sdf = sdf.drop('latitude','longitude')

# COMMAND ----------

q = f'''
select *
from network_dictionary
where country_code = '{cc}'
'''

network_dict = spark.sql(q)

network_dict = network_dict.withColumn('bssid',explode(split(col('bssid_list'),','))).drop('bssid_list','ssid','country_code')

sdf = sdf.join(network_dict,'bssid').drop('bssid').where('network_id > ""').drop_duplicates()

# COMMAND ----------

### step 4

table_name = f'matan_db.ssid_enrichemnt_network_dict_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

sdf.display()

# COMMAND ----------

collect_networks = sdf.groupBy('ip','network_id','latitude','longitude').count()
collect_networks = dbscan_spark(collect_networks, ['ip'], 1,  30000)
collect_networks = collect_networks.withColumnRenamed('count','events')

collect_networks = collect_networks.withColumn('cluster_events',sum_spark('events').over(Window.partitionBy('ip','cluster_id')))\
                                   .withColumn('total_clusters_events',sum_spark('events').over(Window.partitionBy('ip')))\
                                   .withColumn('ratio',col('cluster_events')/col('total_clusters_events'))

max_dist = collect_networks.where('ratio >= 0.8').groupBy('ip','cluster_id').agg(
  min_spark('latitude').alias('min_lat'),
  min_spark('longitude').alias('min_lon'),
  max_spark('latitude').alias('max_lat'),
  max_spark('longitude').alias('max_lon')
)
max_dist = max_dist.selectExpr('ip','cluster_id','distance(min_lat,min_lon,max_lat,max_lon) max_distance').where('max_distance <= 1000')

max_networks = collect_networks.groupBy('ip').agg(countDistinct('network_id').alias('networks'), sum_spark('events').alias('events')).where('networks <= 10')

collect_networks = collect_networks.join(max_dist,['ip','cluster_id'])\
                                   .join(max_networks,'ip')\
                                   .select('ip','network_id').drop_duplicates()

sdf = sdf.join(collect_networks,['ip','network_id'])

# COMMAND ----------

### step 4.1
table_name = f'matan_db.ssid_enrichemnt_collect_networks_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

# Main query
q = f"""
select ip, network_id,
       mean(latitude) latitude,
       mean(longitude) longitude,
       collect_set(ifa) ifas,
       max(timestamp) max_ts,
       min(timestamp) min_ts,
       count(*) as events
from {table_name}
group by 1,2
"""

sdf = spark.sql(q)

# COMMAND ----------

sdf = dbscan_spark(sdf, ['ip'], 0.5,  30000)

# COMMAND ----------

@udf
def list_to_set_count(arr):
  return len(list(set(arr)))

# COMMAND ----------

time_limit_clusters = sdf.groupBy('ip','cluster_id').agg(
  min_spark('min_ts').alias('min_ts'),
  min_spark('max_ts').alias('max_ts'),
  collect_set('ifas').alias('ifas')
)
time_limit_clusters = time_limit_clusters.withColumn('ifas',list_to_set_count(flatten('ifas')))

time_limit_clusters = time_limit_clusters.where(f'''
max_ts - min_ts >= ({min_days_delta_first_last_seen}*24*60*60*1000)    
and ifas >= {min_number_of_ifas_per_ssid}
''')

time_limit_clusters  = time_limit_clusters.select('ip','cluster_id').drop_duplicates()

sdf = sdf.join(time_limit_clusters,['ip','cluster_id'])

sdf = sdf.withColumn('ifas',size('ifas')).drop('max_ts','min_ts','cluster_id')

# COMMAND ----------

@udf
def remove_lst_frst(arr):
  return '_'.join(arr[1:-1])

sdf = sdf.withColumn('ssid',remove_lst_frst(split('network_id','_')))

# COMMAND ----------

### step 5
table_name = f'matan_db.ssid_enrichemnt_time_limit_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

q = f"""
select a.ip, lat1,lon1,lat2,lon2
from
(
  select ip, network_id, latitude lat1, longitude lon1, ssid
  from {table_name}
) a
join
(
  select ip, network_id, latitude lat2, longitude lon2, ssid
  from {table_name}
) b
on a.ip == b.ip
   and a.network_id < b.network_id
   and a.ssid != b.ssid
"""

further_networks_ips = spark.sql(q)
further_networks_ips = further_networks_ips.selectExpr('ip','distance(lat1,lon1,lat2,lon2) distance')
further_networks_ips = further_networks_ips.where('distance > 750').groupBy('ip').count()

# COMMAND ----------

sdf = sdf.join(further_networks_ips,'ip','left').where('count is null').drop('count')

# COMMAND ----------

### step 6
table_name = f'matan_db.ssid_enrichemnt_multi_network_same_ssid_ip_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

sdf = dbscan_spark(sdf, ['ip','ssid'], 0.5,  30000)

# COMMAND ----------

sdf = sdf.withColumn('cluster_events',sum_spark('events').over(Window.partitionBy('ssid','ip','cluster_id')))\
         .withColumn('total_clusters_events',sum_spark('events').over(Window.partitionBy('ssid','ip')))\
         .withColumn('ratio',col('cluster_events')/col('total_clusters_events'))\
         .withColumn('latitude',when(col('ratio')>0.9,col('latitude')))\
         .withColumn('longitude',when(col('ratio')>0.9,col('longitude')))\
         .drop('cluster_events','total_clusters_events','ratio')

# COMMAND ----------

### step 6.1
table_name = f'matan_db.ssid_enrichemnt_multi_clusters_same_ssid_ip_{cc}'
# sdf.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)
sdf = spark.read.table(table_name)

# COMMAND ----------

@udf
def most_frequent(List):
    return max(set(List), key = List.count)

# COMMAND ----------

sdf.display()
spark.read.table(f'matan_db.ssid_enrichemnt_pre_raw_{cc}').display()

# COMMAND ----------

relavent_ssid_ip = spark.read.table(f'matan_db.ssid_enrichemnt_pre_raw_{cc}')
relavent_ssid_ip = relavent_ssid_ip.withColumn('latitude',when( (col('latitude').between(-90,90)) & (col('latitude') != 0) ,col('latitude')))\
                                   .withColumn('longitude',when(col('longitude').between(-180,180) & (col('longitude') != 0) ,col('longitude')))\
                                   .selectExpr('ip as ip_enrch','ssid','latitude as latitude_enrch','longitude as longitude_enrch','ifa')

relavent_ssid_ip_count = relavent_ssid_ip.groupBy(col('ip_enrch').alias('ip')).agg(
  countDistinct('ifa').alias('ifas'),
  count(lit(1)).alias('events'),
  countDistinct('ssid').alias('ssids')
)

relavent_ssid_ip = relavent_ssid_ip.drop('ifa').drop_duplicates()

relavent_ssid_ip = relavent_ssid_ip.join(relavent_ssid_ip_count.where('ssids <= 10').select('ip'),[relavent_ssid_ip_count.ip == relavent_ssid_ip.ip_enrch])

relavent_ssid_ip = sdf.withColumn('ind',lit(True)).join(relavent_ssid_ip,'ssid','left')

relavent_ssid_ip = relavent_ssid_ip.selectExpr('ip_enrch as ip','network_id','latitude','longitude','distance(latitude,longitude,latitude_enrch,longitude_enrch) distance','latitude_enrch','longitude_enrch','ind')
relavent_ssid_ip = relavent_ssid_ip.where('distance<=250 or (ind = True and (distance<=250 or distance is null))').drop('ind')
relavent_ssid_ip = relavent_ssid_ip.groupBy('ip').agg(
                                                        sort_array(collect_list('network_id')).alias('network_id'),
                                                        sort_array(collect_set('network_id')).alias('networks'),
                                                        mean('latitude_enrch').alias('latitude'),
                                                        mean('longitude_enrch').alias('longitude'),
                                                        min_spark('distance').alias('min_distance'),
                                                        max_spark('distance').alias('max_distance'), 
                                                        (ceil_spark(expr('percentile(distance, array(0.75))')[0]/50)*50).alias('accuracy')
                                                     )
relavent_ssid_ip = relavent_ssid_ip.withColumn('network_id', most_frequent('network_id'))
relavent_ssid_ip = relavent_ssid_ip.join(relavent_ssid_ip_count,'ip','left')

# COMMAND ----------

### step 7,8
table_name = f'matan_db.ssid_enrichemnt_final_total_relavent_ips_{cc}'
# relavent_ssid_ip.write.format('delta').option("overwriteSchema", "true").mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

spark.table(table_name).display()

# COMMAND ----------

# phase 1

# --- todo ---

# --- done ---

# add total amount of ips per ssid
# removing top 0.1%  ssids by number of ip

# create the following blacklist -
# $False SSIDs
# '%BlueStacks%','%WiredSSID%','%INVALID%'
# $Hotspot SSID
# '%iPhone%','%Android%','%OPPO%','%Redmi%','%@POCO%','%vivo%','%Galaxy%','%HUAWEI%','%realme%'
# $Home SSIDs 
# '%NOKIA%' , '%Linksys%'  , '%Family%' , '%dlink%' , '%SINGTEL%' , '%MyRepublic%' , '%TP-Link%' , '%Singtel%' , '%ASUS%' , '%Tplink' , '%Askey%' , '%@unifi%' , '%INVALID%' , '%@Maxis%' , '%Dlink%'

# connect netlock SSID->BSSID db (???)
# join on BSSID, Using network_id insead of SSID
# table name - default.network_dictionary

# detection static ip address by searching ip address has small amount of netwroks connected to it and witch are located at the same place.
## How? 
### verify IP has x numvber of networks
### verify the ip has one cluster \ near by cluster center (no more than 200m)

# keep ips has only one cluster.
#########################################################################################################
# phase 2

# --- todo ---

# network name similartiy

# --- done ---

# COMMAND ----------

# statistics
# ip, ssid number of days between events - histogram
