# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 10:53:50 2023

@author: nikhil.upadhyay-v
"""
import numpy as np
import pandas as pd
import psycopg2 
import pandas.io.sql as psql
import sys
import smtplib
import os
import logging
import sqlalchemy
from sqlalchemy import create_engine

def connection_to_database():
    try:
        connection = psycopg2.connect(dbname=dbname_trgt, 
                                      host=host_trgt, 
                                      port=port_trgt,
                                      user=user_trgt, 
                                      password=pwd_trgt)
        print("Connected to the database successfully")
        return connection

    except Exception as e:
        send_mail(e)
        logging.error('%s raised an error', e)
        return None
    
def send_mail(error):
    sender = sender_mail_id
    receivers = reciver_mail_id
    server = smtplib.SMTP('smtp.office365.com', 587)
    try:
        server.ehlo()
        server.starttls()
        server.login(sender, sender_psswd)
        SUBJECT='Update from RSN Script ETL script'
        TEXT=str(error)
        msg = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
        server.sendmail(sender, receivers, msg)
        server.quit()
        print("mail sent successfully")

    except Exception as e:
        print("mail not sent",e)
        
    
connection = psycopg2.connect(dbname= 'dna_ultratech_fare_dev_db',
                        host = 'dna-ultratech-fare-dev.cjyp7b59bidu.ap-south-1.rds.amazonaws.com',
                        port= '5432',
                        user= 'rviplav',
                        password= '45qSTrp4rTR')


class loadData:
    def __init__(self, connection):
        self.connection = connection
        
    def read_data_from_database(self, query):
        return psql.read_sql(query, self.connection).drop_duplicates()
      
     

class RSNCalculation:
    def __init__(self, hierarchyData, zonewiseRaw, plantType, simiFeatures, timeRestriction, baseFreight, talukaop, stock_sale, latestLead):
        self.hierarchyData = hierarchyData
        self.zonewiseRaw = zonewiseRaw
        self.plantType = plantType
        self.simiFeatures = simiFeatures
        self.timeRestriction = timeRestriction
        self.basefreight = baseFreight
        self.talukaop = talukaop
        self.stock_sale = stock_sale
        self.latestLead = latestLead
        
    # def extract_datasets_from_loadDataClass(self):
    #     hierarchyData, zonewiseRaw, plantType, simiFeatures, timeRestriction, baseFreight, talukaop, stock_sale, latestLead = self.data_processor.get_datasets()

                     
    def check_diff_cases_func(self, df_stock): #finding cases with no unique plant/city/segment/truck for single RSN
        checkdiffcases = df_stock.groupby('rsn_no').agg(diffPlant = ('delvry_plant', 'nunique'), diffCity = ('city_code', 'nunique'), diffSegment = ('segment', 'nunique'), diffTruck = ('truck_type', 'nunique'))
        return checkdiffcases
    
    def unique_cases_func(self, df_stock, checkdiffcases): #finding cases with 1 record count for each of plant, city, segment, truck_type
        uniquecases = df_stock[df_stock['rsn_no'].isin(checkdiffcases[checkdiffcases.sum(axis= 1)== 4].index)]
        return uniquecases
    
    def error_cases_func(self, df_stock, checkdiffcases): #finding cases which have multiple records of plant/city/segment/truck for single RSN
        error_cases = df_stock[df_stock['rsn_no'].isin(checkdiffcases[checkdiffcases.sum(axis= 1) != 4].index)]
        return error_cases
        
    
    def quantity_check_func(self, df): #filtering the cases where the quantity lies between 3 to 100 Tonnes
        df = df.groupby(['delvry_plant', 'city_code', 'segment', 'truck_type', 'rsn_no'], as_index = False)['quantity'].sum()
        df = df[df['quantity'].between(3,100)]
        return df
    
    def multiple_plant_cases_func(self, errorcases, checkdiffcases): #finding cases with more than 1 plants
        multiple_plant = errorcases[errorcases['rsn_no'].isin(checkdiffcases[checkdiffcases['diffPlant']>1].index)].merge(df_stock[df_stock.delvry_plant.isin(errorcases[errorcases['rsn_no'].isin(checkdiffcases[checkdiffcases['diffPlant']>1].index)].delvry_plant.unique())].groupby(
                'delvry_plant', as_index = False)['quantity'].sum().rename(columns = {'quantity':'totDispQty'}), how ='left')
        
        max_index = multiple_plant.groupby(['rsn_no'])['totDispQty'].transform('idxmax')  # finding the index of the delvryplant where total dispatch quantity is greatest grouped at rsn no
        multiple_plant['new_plant']=multiple_plant.loc[max_index,'delvry_plant'].values
        multiple_plant = multiple_plant.rename(columns={'new_plant':'delvry_plant'}).drop('totDispQty', axis= 1)
        return multiple_plant
    
    def concat_single_multiple_plant_cases_func(self, df1, checkdiffcases): # concatenating single and multiple plant cases
        df1 = pd.concat([df1[~df1.rsn_no.isin(checkDistCases[checkDistCases.distPlant > 1].index)], mul_plant])
        df1['VehicleSize'] = df1.groupby('rsn_no')['quantity'].transform('sum') # Summing quantity at RSN level now
               
        df1 = df1.groupby(['delvry_plant', 'city_code', 'segment', 'rsn_no', 'VehicleSize'], as_index = False).agg(
                            truck_type = ('truck_type', 'max')).rename(columns = {'VehicleSize' : 'quantity'})
        df1 = df1[df1['quantity'].between(3, 100)] # Cases with truck size less than 3MT or more than 100MT are discarded
        return df1

    def concat_unique_multiple_cases(self, uniquecases, multiplecases): #concatenating unique and multiple cases
        return pd.concat(uniquecases, multiplecases)
    
    def calc_total_quantity(self, df): # calculate total quantity at Plant, City, Truck, 'Vehicle Size'. Vehicle Size is quantity summed up at RSN level
        df_total_quantity = df.groupby(['delvry_plant', 'city_code', 'truck_type', 'quantity'], as_index= False).agg(total_quantity = ('quantity', 'sum'))
        return df_total_quantity
    
    
    def process_stock_data(self):
        df_stock =self.stock_sale.drop_duplicates(subset= 'document_no', keep = 'first').drop('document_no', axis= 1) # Removing duplicate cases based on unique document numbers only
        checkdiffcases = self.check_diff_cases_func(df_stock)
        uniquecases = self.unique_cases_func(df_stock, checkdiffcases)
        uniquecases = self.quantity_check_func(uniquecases)
        errorcases = self.error_cases_func(df_stock, checkdiffcases)
        multiple_plant = self.multiple_plant_cases_func(errorcases, checkdiffcases)
        errorcases = self.concat_single_multiple_plant_cases_func(errorcases, checkdiffcases)
        stock_RSN = self.concat_unique_multiple_cases(uniquecases, errorcases)
        df_total_quantity = self.calc_total_quantity(stock_RSN)
        return df_total_quantity
    
    def clean_base_freight_data(self): #cleaning the base freight dataset
        bf_filtered = self.baseFreight[(~self.baseFreight['source_sloc'].str.endswith('R', na= False)) & (self.baseFreight["base_freight"] != 1) & (self.baseFreight['plant_code'].str.startswith('69',na = False))]
        bf_filtered['base_freight'] = bf_filtered['base_freight'].apply(np.floor)
        bf_filtered['validity_start'] = pd.to_datetime(bf_filtered.validity_start)

        # for picking up latest basefreight based on validity start date
        max_index = bf_filtered.groupby(['plant_code','city_code','special_process_indi'])['validity_start'].transform('idxmax')
        bf_filtered['newbf'] = bf_filtered.loc[max_index,'base_freight'].values
        bf_filtered.rename(columns = {'base_freight':'old_bf','newbf':'base_freight'},inplace = True)
        bf_clean = bf_filtered[['plant_code','city_code','base_freight','special_process_indi','toll_rate','unloading']]
        bf_clean = bf_clean.drop_duplicates(subset = ['plant_code','city_code','special_process_indi','base_freight'],keep = 'first')
        bf_clean.rename(columns = {'plant_code':'delvry_plant','special_process_indi':'truck_type'},inplace = True)
        return bf_clean
        
        
    def merge_basefreight_quantity_data(self):
        df_total_quantity = self.process_stock_data(self.stock_sale)
        bf_clean = self.clean_base_freight_data(self.baseFreight)
        df_base_freight = pd.merge(bf_clean, df_total_quantity, how = 'right', on= ['delvry_plant', 'city_code', 'truck_type'])
        df_base_freight = df_base_freight[['delvry_plant', 'city_code', 'truck_type', 'base_freight', 'toll_rate', 'unloading', 'total_quantity', 'quantity']].drop_duplicates()
        return df_base_freight

    def basefreight_lead(self, df1, df2):
        df2 = df2.merge(df1.drop('date1', 1).drop_duplicates(), how = 'left').rename(columns = {'lead_km' : 'lead'})
        return df2
    
    def calc_ptpk(self, df_base_freight):
        df_base_freight['ptpk'] = df_base_freight['base_freight']/df_base_freight['lead']
        return df_base_freight
   
    def merge_plant_zone_data(self, df1, df2):
        df_zone = df1.merge(df2.rename(columns= {'code': 'delvry_plant'}), how = 'left')
        return df_zone
    
    def merge_zone_hierarchy_time(self, df_base_freight):
        df_zone = self.merge_plant_zone_data(df_base_freight, self.zonewiseRaw)
        df_hierarchy = df_zone.merge(self.hierarchyData, how = 'left', on= 'city_code').drop_duplicates()
        self.timeRestriction['plant'] = self.timeRestriction['plant'].astype(str)
        time_restriction_clean = self.timeRestriction[['plant','city_code','total_restriction_time', 'integration_flag', 'cluster_flag','union_flag']].drop_duplicates(subset = ['plant','city_code'],keep = 'first').rename(columns = {'plant':'delvry_plant'})
        df_time_restriction = df_hierarchy.merge(time_restriction_clean, how = 'left', on= ['delvry_plant','city_code']).drop_duplicates()
        return df_time_restriction
    
    def add_route_features(self, df_time_restriction): #adding the route features, type of 'bag' or 'bulk'
        self.simiFeatures['plant'] = self.simiFeatures['plant'].astype(str)
        features_clean = self.simiFeatures.drop_duplicates(subset = ['plant', 'city_code'], keep = 'first')[['plant','city_code','nh_per','sh_per','plain_per','hilly_per']]
        df_features = df_time_restriction.merge(features_clean.rename(columns = {'plant':'delvry_plant'}),how = 'left',on = ['delvry_plant','city_code'])
        df_PlantType = df_features.merge(plantType.rename(columns= {'plant':'delvry_plant','type':'plant_type'}), how = 'left',on = 'delvry_plant')
        df_PlantType['total_restriction_time'] = np.where(df_PlantType['total_restriction_time'].isna(), 0, df_PlantType['total_restriction_time'])
        df_PlantType['type'] = np.where(df_PlantType['truck_type'].str.startswith(('12','5','7')),'Bulk', np.where(df_PlantType['truck_type'].str.startswith(('1','2')),'Bag','Bulk'))
        return df_PlantType
        
    def integrate_taluka_output(self):
        self.talukaop = self.talukaop.rename(columns= {'plant': 'delvry_plant'})
        self.talukaop['delcry_plant'] = self.talukaop['delvry_plant'].astype(str)
        talukaop_subset=self.talukaop[['delvry_plant','city_code','truck_type','direct_sto', 'remarks', 'monthly_average_impact']].drop_duplicates().rename(columns={'monthly_average_impact':'MonthlyAvgImpact(Taluka Analysis)','remarks':'Remarks(Taluka Analysis)'})
        return talukaop_subset
    
    def merge_plant_taluka_data(self, df_PlantType, talukaop_subset):
        all_merged = df_PlantType.merge(talukaop_subset, on=['delvry_plant','city_code','truck_type'],how='left')
        return all_merged
    
    def final_result(self):
        df_base_freight = self.merge_basefreight_quantity_data()
        df_base_freight = self.calc_ptpk(df_base_freight)
        df_time_restriction = self.merge_plant_zone_data(df_base_freight)
        df_PlantType = self.add_route_features(df_time_restriction)
        talukaop_subset = self.integrate_taluka_output()
        all_merged = self.merge_plant_taluka_data(df_PlantType, talukaop_subset)
        
        Plant_City_Truck_3Month_dispatch = all_merged[['delvry_plant','city_code','truck_type','quantity', 'total_quantity', 'lead',
                                                  'nh_per','sh_per','ptpk','plain_per','hilly_per', 'total_restriction_time', 'direct_sto',
                                                  'type','zone','integration_flag', 'cluster_flag','union_flag', 'base_freight','plant_type',
                                                  'district_code', 'district_desc','depot_code', 'depot_desc','region_code','region_desc', 
                                                  'i2_taluka','i2_taluka_desc','city_desc','plant_name',
                                                  'Remarks(Taluka Analysis)','MonthlyAvgImpact(Taluka Analysis)']]
        return Plant_City_Truck_3Month_dispatch

    

class PGTableLoader:
    def __int__(self, connection):
        self.connection = connection
        
    def load_data_to_table(self, df, table_name):
        df.to_csv('temp_rsn_cal.csv', index= None)
        
        with open('temp_rsn_cal.csv', 'r') as file:
            connection = psycopg2.connect(dbname= 'dna_ultratech_fare_dev_db',
                                    host = 'dna-ultratech-fare-dev.cjyp7b59bidu.ap-south-1.rds.amazonaws.com',
                                    port= '5432',
                                    user= 'rviplav',
                                    password= '45qSTrp4rTR')
            
            cur = connection.cursor()
            cur.execute("Truncate {} Cascade;".format(table_name)) #truncate the table before loading new data
            print("Truncated {}".format(table_name))
            cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), file) # load data from the csv file into the database table
            cur.execute("commit;")
            print("Loaded data into {}".format(table_name))
        # self.connection.close() # close the database connection
        except Exception as e:
            send_mail(e)
            logging.error('% raised an error', e)
            


if __name__ == '__main__':
    print("UTCL Copy::ETL started at ----------->>>>>>>>")

    #extracting the server details
    env_map = {
    'DEV': (dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt),
    'QA': (dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt),
    'UAT': (dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt),
    'PROD_V2': (dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt),
    'PROD': (dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt)
    }

    if sys.argv[1] in env_map:
        dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt = env_map[sys.argv[1]]
    else:
        print("no env found")    
    
    
    
    fetch_data = loadData(connection) #creating the object of the class loadData by using the connection to the server
    
    #commmands to fetch data from the database
    hierarchyData = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.vw_hierarchy_data_stg')
    hierarchyData.to_csv('hierarchy.csv')
    zonewiseRaw = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.zonewise_raw')
    zonewiseRaw.to_csv('zonewiseRaw.csv')
    plantType = fetch_data.read_data_from_database('SELECT * from dna_ultratech_fare_dev.plant_type')
    plantType.to_csv('plantType.csv')
    simiFeatures = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.simi_features_raw')
    simiFeatures.to_csv('simiFeatures.csv')
    timeRestriction = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.time_restriction')
    timeRestriction.to_csv('timeRestriction.csv')
    baseFreight = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.vw_dna_base_freight_stg')
    baseFreight.to_csv('baseFreight.csv')
    talukaop = fetch_data.read_data_from_database('SELECT * FROM dna_ultratech_fare_dev.taluka_dashboard_output where last_refresh_date = (select max(last_refresh_date) from dna_ultratech_fare_dev.taluka_dashboard_output )')
    talukaop.to_csv('talukap.csv')
    stock_sale = fetch_data.read_data_from_database("select document_no, delvry_plant,city_code,segment, truck_type,rsn_no,quantity from dna_ultratech_fare_dev.vw_direct_stock_sale_stg where date1  >  CURRENT_DATE - INTERVAL '3 months' and delvry_plant  like '69%%' and avg_frt > 1 and lead_km > 0 and tmode like 'ROAD' and rsn_no is not null and rsn_no not like '{%'")
    stock_sale.to_csv('stock_sale.csv')
    latestLead = fetch_data.read_data_from_database("""select t.delvry_plant,t.city_code,t.truck_type,t.lead_km,t.date1 from (
              select rank() OVER(PARTITION BY delvry_plant,city_code,truck_type order by date1 desc) rn,
            delvry_plant, city_code, truck_type, lead_km, date1 from dna_ultratech_fare_dev.vw_direct_stock_sale_stg where
            date1  >  CURRENT_DATE - INTERVAL '3 months' and delvry_plant like '69%%' and avg_frt > 1 and lead_km > 0 and 
            tmode like 'ROAD' and rsn_no is not null and rsn_no not like '{%') t where t.rn = 1""")
    latestLead.to_csv('latestLead.csv')
 
    
   
     rsn = RSNCalculation(hierarchyData, zonewiseRaw, plantType, simiFeatures, timeRestriction, baseFreight, talukaop, stock_sale, latestLead)
     #creating the object of another class RSNCalculation to perform the functions on the fetched datasets
    
    
     df_total_quantity = rsn.process_stock_data()
     
     
     rsn.final_result()
