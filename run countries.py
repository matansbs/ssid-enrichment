# Databricks notebook source
counties = ['SGP','MYS','LBN','PSE','IDN','TUR']

# COMMAND ----------

for c in counties:
  print(f'run {c} starts.')
  dbutils.notebook.run('./ssid-enrichment',0, {'country': c} )
  print(f'run {c} done.')
  print('*'*50)
