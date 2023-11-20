# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 13:40:41 2022

@author: viplav.singla-v
"""

import numpy as np
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql


connection = pg.connect("host=dna-ultratech-fare-dev.cjyp7b59bidu.ap-south-1.rds.amazonaws.com dbname=dna_ultratech_fare_dev_db user=rviplav password=45qSTrp4rTR")
heirarchyData = psql.read_sql('SELECT * FROM dna_ultratech_fare_dev.vw_hierarchy_data_stg', connection).drop_duplicates()
stock_sale=psql.read_sql("""select delvry_plant || '-' || city_code || '-' || truck_type as code,
                              delvry_plant || '-' || city_code as code2,	city_code,delvry_plant,truck_type,sum(quantity) as quantity,
 		                     max(lead_km) as lead_km from dna_ultratech_fare_dev.vw_direct_stock_sale_stg dss where
                    		truck_type not like '3%'
                    		and lead_km >1
                            and delvry_plant  like '69%%' 
                    		and tmode like 'RO%'
                            and date1 between  '09-16-2022' and '12-15-2022'
                            group by code,city_code,delvry_plant,truck_type""",connection)                               

# stock_sale = psql.read_sql('select distinct(date1) from dna_ultratech_fare_dev.vw_direct_stock_sale_stg dss',connection)
# stock_sale.head()

# Taking lead separately for last 3 months valid dispatch data based on latest lead available
latestLead = psql.read_sql("""select t.delvry_plant,t.city_code,t.truck_type,t.lead_km,t.date1 from (
    select rank() OVER(PARTITION BY delvry_plant,city_code,truck_type order by date1 desc) rn,
    delvry_plant, city_code, truck_type, lead_km, date1 from dna_ultratech_fare_dev.vw_direct_stock_sale_stg where
    date1 between  '09-16-2022' and '12-15-2022' and delvry_plant like '69%%' and avg_frt > 1 and lead_km >1 and 
    tmode like 'ROAD') t where t.rn = 1""", connection)

# latestLead = psql.read_sql('select distinct(date1) from dna_ultratech_fare_dev.vw_direct_stock_sale_stg',connection)


baseFreight=psql.read_sql('SELECT * FROM dna_ultratech_fare_dev.vw_dna_base_freight_stg', connection).drop_duplicates()

   
# Cleaning Base Freight data
bf_filtered = baseFreight[(~baseFreight['source_sloc'].str.endswith('R',na = False))&(baseFreight["base_freight"] != 1)&(baseFreight['plant_code'].str.startswith('69',na = False))]
bf_filtered['base_freight'] = bf_filtered['base_freight'].apply(np.floor)
bf_filtered['validity_start'] = pd.to_datetime(bf_filtered.validity_start)

# for picking up latest basefreight based on validity start date
max_index = bf_filtered.groupby(['plant_code','city_code','special_process_indi'])['validity_start'].transform('idxmax')
bf_filtered['newbf'] = bf_filtered.loc[max_index,'base_freight'].values
bf_filtered.rename(columns = {'base_freight':'old_bf','newbf':'base_freight'},inplace = True)
bf_clean = bf_filtered[['plant_code','city_code','base_freight','special_process_indi','validity_start','validity_end','toll_rate','unloading']]
bf_clean = bf_clean.drop_duplicates(subset = ['plant_code','city_code','special_process_indi','base_freight'],keep = 'first')
bf_clean.rename(columns = {'plant_code':'delvry_plant','special_process_indi':'truck_type'},inplace = True)

# merging the basefreight 
df_base_freight = pd.merge(bf_clean,stock_sale,how = 'right',on = ['delvry_plant','city_code','truck_type'])

# bringing lead on plant, city and truck level 
df_base_freight = df_base_freight.merge(latestLead.drop('date1', 1).drop_duplicates(), how = 'left').rename(columns = {'lead_km' : 'lead'})

# Removing cases for whom dispatch data is present but not Base Freight data
df_base_freight1 = df_base_freight[~df_base_freight.base_freight.isna()]

# df_base_freight1['quantity'].sum()

# adding the heirarchy data 
df_heirarchy = df_base_freight1.merge(heirarchyData,how = 'left',on = 'city_code').drop_duplicates()

df_heirarchy=df_heirarchy[['delvry_plant', 'city_code', 'truck_type','quantity','state_desc', 'district_desc','i2_taluka',
                           'i2_taluka_desc','city_desc', 'lead','base_freight','toll_rate','unloading','validity_start',
                           'validity_end']]

# calculating ptpk 
df_heirarchy['ptpk'] = df_heirarchy['base_freight']/df_heirarchy['lead']

# adding drect sto column 
df_heirarchy['direct_sto'] = np.where((df_heirarchy['city_desc'].str.endswith('STO') == True),'STO','DIRECT')

# Input for taluka 
dispatchdata_newdestination_org_with_validity_dates = df_heirarchy.copy()
dispatchdata_newdestination_org_with_validity_dates['i2_taluka_desc'] = dispatchdata_newdestination_org_with_validity_dates['i2_taluka_desc'].str.replace(" ", "")
# dispatchdata_newdestination_org_with_validity_dates['quantity'].sum()

dispatchdata_newdestination_org_with_validity_dates.to_csv('dispatchdata_newdestination_org_with_validity_dates_16_09_to_15_12.csv')
dispatchdata_newdestination_org_with_validity_dates_database=psql.read_sql('select * from dna_ultratech_fare_dev.dispatchdata_newdestination_org_with_validity_dates',connection).drop_duplicates().reset_index(drop=True)
