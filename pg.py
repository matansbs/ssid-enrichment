# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_multi_clusters_same_ssid_ip_lbn
# MAGIC where network_id like '"LBN_dw%'
# MAGIC order by ifas desc

# COMMAND ----------

(1664267460000 - 1650852599000 ) / ( 24*60*60*1000 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_collect_networks_
# MAGIC where ip = '91.192.176.6'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_time_limit_lbn
# MAGIC where ip = '91.192.176.6'

# COMMAND ----------

7303 / (7303 + 191)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_pse
# MAGIC order by max_distance desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_sgp
# MAGIC order by max_distance desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_idn
# MAGIC order by max_distance desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_tur
# MAGIC order by ifas desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_idn
# MAGIC order by max_distance desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select net,events, array_sort(ips) ips
# MAGIC from
# MAGIC (
# MAGIC select explode(network_id) net, sum(events) events, collect_set(ip) ips
# MAGIC from matan_db.ssid_enrichemnt_final_total_relavent_ips_lbn
# MAGIC group by 1
# MAGIC )
# MAGIC where size(ips) > 3
# MAGIC order by 2 desc

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*, latitude, longitude
# MAGIC from
# MAGIC (
# MAGIC     select 
# MAGIC     ip,
# MAGIC     ssid,
# MAGIC     bssid,
# MAGIC     ifa,
# MAGIC     timestamp
# MAGIC     from matan_db.ssid_enrichemnt_pre_raw_lbn_185_187_95_158
# MAGIC ) a
# MAGIC join
# MAGIC (
# MAGIC   select
# MAGIC   bssid,
# MAGIC   mean(latitude) latitude,
# MAGIC   mean(longitude) longitude
# MAGIC   from bssid_dictionary_new
# MAGIC   where country_code = 'LBN'
# MAGIC   group by 1
# MAGIC ) b
# MAGIC 
# MAGIC using (bssid)

# COMMAND ----------

# MAGIC %sql
# MAGIC select ssid,count(distinct nullif(latitude,0), nullif(longitude,0)) locations, count(distinct ifa) ifas, count(distinct timestamp)
# MAGIC from sdk_prod
# MAGIC where country_code = 'LBN'
# MAGIC       and ssid > ''
# MAGIC       and ip = "185.187.95.158"
# MAGIC       and ptime >= 2022110000
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select ssid, count(*) events, count(distinct latitude, longitude) locations, count(distinct ifa) ifas, count(distinct timestamp)
# MAGIC from (
# MAGIC select a.*, latitude, longitude
# MAGIC from
# MAGIC (
# MAGIC     select 
# MAGIC     ip,
# MAGIC     ssid,
# MAGIC     bssid,
# MAGIC     ifa,
# MAGIC     timestamp
# MAGIC     from matan_db.ssid_enrichemnt_pre_raw_lbn_185_187_95_158
# MAGIC ) a
# MAGIC join
# MAGIC (
# MAGIC   select
# MAGIC   bssid,
# MAGIC   mean(latitude) latitude,
# MAGIC   mean(longitude) longitude
# MAGIC   from bssid_dictionary_new
# MAGIC   where country_code = 'LBN'
# MAGIC   group by 1
# MAGIC ) b
# MAGIC 
# MAGIC using (bssid)
# MAGIC )
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from syitzhak_db.network_dictionary
# MAGIC where country_code = 'SGP'
# MAGIC       and split(network_id,'_')[size(split(network_id,'_'))-1] >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from syitzhak_db.network_dictionary
# MAGIC where country_code = 'SGP'
# MAGIC       and split(network_id,'_')[size(split(network_id,'_'))-1] < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from network_dictionary
# MAGIC where country_code = 'SGP'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from network_dictionary
# MAGIC where country_code = 'SGP'
# MAGIC       and ssid = 'Wireless@SGx'
