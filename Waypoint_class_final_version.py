# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 10:59:47 2023

@author: nikhil.upadhyay-v
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 12:33:16 2023

@author: nikhil.upadhyay-v
"""

import numpy as np
import pandas as pd
from haversine import haversine_vector, Unit
import psycopg2 # to connect to the database
from configparser import ConfigParser
import pandas.io.sql as psql
from os import listdir
from os.path import isfile, join
import re
from geopy.geocoders import Nominatim
import os
import pdb
print("initiliazing")


# function to connect to the database by giving user name & password to both production & development host
def connection_to_database(database_name, host_name, port, user_name, user_password):
    connection = psycopg2.connect(
        dbname = database_name,
        host = host_name,
        port = port,
        user = user_name,
        # connection_timeout = 30,
        password = user_password      
        )
    
    return connection

def Unique_list_tripid_GPS():
    cur = connection_to_database(database_name_dev,host_name_dev,port_dev,user_name_dev,user_password_dev)
    cursor = cur.cursor()
    cursor.execute('select distinct(trip_id) from dna_ultratech_fare_dev.vw_gps_location_details_monitered_trip_jan')    
    col = [desc[0] for desc in cursor.description]
    unique_trip_id_in_GPS_data = cursor.fetchall()
    unique_trip_id_in_GPS_data = pd.DataFrame(unique_trip_id_in_GPS_data, columns=col)
    return unique_trip_id_in_GPS_data


#class to fetch the data based on sql commands and return the dataframe
class loadData:
    
    def __init__(self, cur):
        self.cursor = cur.cursor()
        
    def fetch_data(self, sql):
        self.cursor.execute(sql)
        col = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        if len(data) > 0:
            print('Good to Go')
            
        return pd.DataFrame(data, columns = col)
    
        
class dataProcessing:
    
    def __init__(self, facility_master, node_facility_master, customer_master, node_customer_master, node_address):
        self.facility_master = facility_master
        self.node_facility_master = node_facility_master
        self.customer_master = customer_master
        self.node_customer_master = node_customer_master
        self.node_address = node_address
           
        
    def dataPreparation(self): #to fetch lat long, we need to have the details of both plant side and customer side
        
        #data preparation for plant details obtained from the server
        Facility_Master = pd.merge(
            self.node_facility_master[['facility', 'facility_code', 'facility_desc', 'facility_processing', 'facility_sub_type', 'validated_by', 'i2_code', 'address_id_id']],
            self.node_address[['id', 'lat', 'lng']], how='left', left_on='address_id_id', right_on='id'
        ).drop(columns=['address_id_id', 'id']).merge(
            self.facility_master[['facility_code', 'depot_code', 'depot_desc']], how='left', on='facility_code'
        ).rename(columns={'depot_desc': 'SLOC_DEPOT', 'facility_desc': 'SLOC_DESC', 'facility_processing': 'WH_TYPE', 'i2_code': 'FACILITY_KEY'})
            
        FcltyLtLng = Facility_Master[['SLOC_DEPOT','SLOC_DESC','facility_code','lat','lng','WH_TYPE']].rename(
            columns={'SLOC_DESC':'DESCR','facility_code':'KEY','WH_TYPE':'Identifier'}).assign(DEPOT_DESCRIPTION= lambda x:x['DESCR'], district_description= lambda x:x['DESCR'], taluka_descr= lambda x:x['DESCR'])

            
        #data preparation for customer details obtained from the server
        Cust_Master = pd.merge(self.node_customer_master[['customer_code', 'is_active', 'validated_by', 'address_id_id']], self.node_address[[
                               'id', 'lat', 'lng']], how='left', left_on='address_id_id', right_on='id'
            ).drop(columns= ['address_id_id', 'id']).merge(
                self.customer_master[['city_code', 'city_descr', 'taluka_code', 'taluka_descr', 'district_code', 'district_description', 'depot_code', 'depot_description', 'region_code', 'region_description', 'state_code', 'state_description', 'zone_code', 'zone_descr', 'country', 'active']], 
                how='left', left_on='customer_code', right_on='city_code').drop(columns= ['is_active']).rename(
                    columns={'city_descr': 'CITY_DESCR','depot_description': 'DEPOT_DESCRIPTION'})
                    
        CustLtLng = Cust_Master[['DEPOT_DESCRIPTION', 'district_description', 'taluka_descr','CITY_DESCR','customer_code','lat','lng']].assign(Identifier= 'CUSTOMER').rename(
            columns={'CITY_DESCR': 'DESCR', 'customer_code': 'KEY'})
        
        
        Waypoint_WCustFclty_WLatLng = pd.concat([FcltyLtLng, CustLtLng], ignore_index= True).drop_duplicates()
        
        return Waypoint_WCustFclty_WLatLng    
    
    
    def importing_file(self, MonitoredTripGPSData_Input_mypath): #concatenating into a single file consisting of the data from all the files in the specified folder
        onlyfiles = pd.DataFrame([f for f in listdir(MonitoredTripGPSData_Input_mypath) if isfile(join(MonitoredTripGPSData_Input_mypath, f))])
        onlyfiles.columns = ['FName']
        Output = pd.DataFrame()
        FName = onlyfiles[(onlyfiles['FName'].str.contains(".xlsx")) | (onlyfiles['FName'].str.contains(".csv"))].reset_index().drop(columns= ['index'])
        
        for i in range(len(FName)):
            InputFile = MonitoredTripGPSData_Input_mypath + FName['FName'].values[i]
            if  len(re.compile(r'\b' + r'\S*' + re.escape('.xlsx') + r'\S*').findall(InputFile)) > 0: Output = pd.concat([Output,pd.read_excel(InputFile, thousands=",",sheet_name = 'Consignment Report',engine='openpyxl')],ignore_index=True)
            if  len(re.compile(r'\b' + r'\S*' + re.escape('.csv') + r'\S*').findall(InputFile)) > 0: Output = pd.concat([Output,pd.read_csv(InputFile, thousands=",",sheet_name = 'Consignment Report')],ignore_index=True)
                  
        return Output
    
        
    def importing_monitored_trip_data(self, MonthRun, MonitoredTripGPSData_Input_mypath): #want to use Output (return of last function) here
    
        MonitoredTrip_Axestrack_LtstMnth = pd.DataFrame()
        MonitoredTrip_Axestrack = pd.DataFrame()
        # MonitoredTrip_Axestrack = self.importing_file(MonitoredTripGPSData_Input_mypath)
        MonitoredTrip_Axestrack = Output.copy()
        MonitoredTrip_Axestrack['Month'] = MonthRun
        # Output['Plant'].info()
        # Output.to_csv('Output.csv')
        MonitoredTrip_Axestrack.rename(columns={'SAP Lead Distance (Kms)':'Lead_SAP'},inplace=True)
        # MonitoredTrip_Axestrack = MonitoredTrip_Axestrack.astype({'Plant2': str, 'Trip ID': str, 'DI No': str })
        MonitoredTrip_Axestrack['Plant Code'] = MonitoredTrip_Axestrack['Plant Code'].astype(str)
        MonitoredTrip_Axestrack['Trip ID'] = MonitoredTrip_Axestrack['Trip ID'].astype(str)
        MonitoredTrip_Axestrack['DI No'] = MonitoredTrip_Axestrack['DI No'].astype(str)
        
        #filtering only the monitored trip IDs
        MonitoredTrip_Axestrack = MonitoredTrip_Axestrack[(MonitoredTrip_Axestrack['Accountability'] == 'Monitored')|(MonitoredTrip_Axestrack['Trip Status'] == 'TRIP MONITORED')][['Month','Trip ID','Plant Code','Depot','City Code','DI No','Lead_SAP']]
        MonitoredTrip_Axestrack.reset_index(drop= True, inplace= True)
        MonitoredTrip_Axestrack_LtstMnth = pd.concat([MonitoredTrip_Axestrack_LtstMnth,MonitoredTrip_Axestrack],ignore_index=True)
        MonitoredTrip_Axestrack_LtstMnth = pd.merge(MonitoredTrip_Axestrack_LtstMnth, self.customer_master[['city_code','depot_description','district_description','taluka_descr','city_descr','state_description']],how='left',left_on='City Code',right_on = 'city_code')
        # MonitoredTrip_Axestrack_LtstMnth.rename(columns={'Plant Code': 'Plant'}, inplace= True)
        # MonitoredTrip_Axestrack_LtstMnth = MonitoredTrip_Axestrack_LtstMnth.astype({'Plant': str, 'Trip ID': str, 'DI No': str })
        #filtering the undesirable plant codes
        MonitoredTrip_Axestrack_LtstMnth['Plant Code'] = MonitoredTrip_Axestrack_LtstMnth['Plant Code'].str.slice(0, 4)
        dataj = MonitoredTrip_Axestrack_LtstMnth[~MonitoredTrip_Axestrack_LtstMnth['Plant Code'].isin(['SD01', 'KC01', 'GK01', 'BA01'])]
        # dataj.to_csv(r'C:\Users\nikhil.upadhyay-v\Downloads\SED\New folder\dataj.csv')
        return dataj
    
    
class waypoint:
    
    def __init__(self, cur, dataj, unique_tripID_monitored, unique_tripID_GPS_data, Waypoint_WCustFclty_WLatLng, geolocator): #initialization function
        self.cursor = cur.cursor()
        self.dataj = dataj
        self.unique_tripID_monitored = unique_tripID_monitored
        self.unique_tripID_GPS_data = unique_tripID_GPS_data
        self.Waypoint_WCustFclty_WLatLng = Waypoint_WCustFclty_WLatLng
        self.geolocator = geolocator
        
        
    def fetch_GPS_data(self, trip_list):
        conn = connection_to_database(database_name_dev,host_name_dev,port_dev,user_name_dev,user_password_dev)
        cur = conn.cursor()
        #Fetch GPS ping data from DB for all the unique TIDs
        l = trip_list
        l = tuple(l)
        
        params = {'l': l}
        
        cur.execute('select * from dna_ultratech_fare_dev.vw_gps_location_details_monitered_trip_jan where trip_id in %(l)s',params)    
        
        col = [desc[0] for desc in cur.description]
        GPS_location_data = cur.fetchall()
        GPS_location_data = pd.DataFrame(GPS_location_data, columns=col)
        return GPS_location_data
    
        
    def common_tripID_func(self): #finding out the common trip IDs
        common_tripID = self.unique_tripID_monitored.merge(self.unique_tripID_GPS_data, on = 'Trip ID', how= 'inner').merge(dataj, on = 'Trip ID', how= 'inner')
        return common_tripID
    
    def unique_plant_taluka_combination_func(self,common_tripID): #grouping the common trip ID by group & taluka to form its unique combination
        common_tripID_whole_Data = self.dataj.loc[self.dataj['Trip ID'].isin(set(common_tripID['Trip ID']))].reset_index(drop=True)
        unique_plant_taluka_combination = common_tripID_whole_Data.groupby(['Plant Code','taluka_descr'],as_index=False)['Trip ID'].agg("count").sort_values(['Trip ID']).reset_index(drop=True)
        return common_tripID_whole_Data, unique_plant_taluka_combination
    
        
    def trip_list_func(self, common_tripID_whole_Data, unique_plant_taluka_combination, ctr):
        trip_list = common_tripID_whole_Data.loc[(common_tripID_whole_Data['Plant Code']==unique_plant_taluka_combination['Plant Code'][ctr]) & (common_tripID_whole_Data['taluka_descr']==unique_plant_taluka_combination['taluka_descr'][ctr]) & (common_tripID_whole_Data['Lead_SAP'] > 40)]['Trip ID'].unique()
        return trip_list    
    
    
    def check2_GPS_trip_location_details(self, GPS_trip_location_details, trip_list, dataj,unique_plant_taluka_combination, ctr):
        dataj_ = dataj[(dataj['Plant Code']==unique_plant_taluka_combination.loc[ctr,'Plant Code']) & (dataj['taluka_descr']==unique_plant_taluka_combination.loc[ctr,'taluka_descr'])]          
        dataj_ = dataj_.sort_values(by=['Lead_SAP'],ascending = False)
        dataj_ = dataj_.drop_duplicates(subset=['Trip ID'],keep='first').reset_index(drop=True)
        return dataj_
    
    def Axestrack_MergeGPS_func(self, dataj_, GPS_trip_location_details):
        GPS_trip_location_details.rename(columns = {'trip_id': 'Trip ID'}, inplace= True)
        Axestrack_MergeGPS = pd.merge(dataj_, GPS_trip_location_details, on = 'Trip ID', how= 'inner')
        
        #combining Axestrack with the plant lat long details, which is the origin
        Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS, Waypoint_WCustFclty_WLatLng[['KEY','lat','lng']],left_on='Plant Code',right_on='KEY',how='left').rename(columns= {'lat': 'Orgn_lat', 'lng': 'Orgn_lng'}).drop(columns= ['KEY'])
             
        #combining Axestrack with the city lat long details, which is the destination
        Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS, Waypoint_WCustFclty_WLatLng[['KEY','lat','lng']],left_on='City Code',right_on='KEY',how='left').drop(columns= ['KEY']).rename(columns= {'lat': 'Dest_lat', 'lng': 'Dest_lng'})
        
        Axestrack_MergeGPS['DateTimeSeq'] = pd.to_datetime(Axestrack_MergeGPS['lat_long_time'], format='%d%m%Y%H%M%S') #converting to datetime
        
        return Axestrack_MergeGPS
    
    def count_trips_latlng_func(self, GPS_trip_location_details):
        count_trips_wrt_latlng = GPS_trip_location_details.groupby(['Trip ID'], as_index= False)['lat_long_time'].agg("count")
        count_trips_wrt_latlng.rename(columns= {'lat_long_time': 'Total_GPS_pings_per_tripID'}, inplace= True)             
        return count_trips_wrt_latlng
    
    
    def check3_unique_trips_count(self, total, unique_trips_count, Axestrack_MergeGPS):
        df = Axestrack_MergeGPS[Axestrack_MergeGPS['Trip ID'] == unique_trips_count[total]].reset_index(drop = True)
        df2 = pd.DataFrame([[1,df.loc[0,'Trip ID'],df.loc[0,'Plant Code'],df.loc[0,'City Code'],df.loc[0,'Lead_SAP'],df.loc[0,'Total_GPS_pings_per_tripID'],df.loc[0,'depot_description'],df.loc[0,'district_description'],df.loc[0,'taluka_descr'],df.loc[0,'city_descr']]],columns= ['Key','Trip ID','Plant','City Code','Lead','TripID_rcrdCnt','depot_description','district_description','taluka_descr','city_descr'])
        df2['HvrsineAggrtd'] = 0
        return df2
    
    def check3_trip_wayroute_details(self, total,unique_trips_count,Axestrack_MergeGPS):
        df = Axestrack_MergeGPS[Axestrack_MergeGPS['Trip ID'] == unique_trips_count[total]].reset_index(drop = True)
        trip_wayroute_details = df[['Orgn_lat','Orgn_lng','Dest_lat','Dest_lng','latitude','longitude','DateTimeSeq','Depot','depot_description','district_description','taluka_descr','city_descr']].drop_duplicates().sort_values(by= 'DateTimeSeq').reset_index(drop= True)
        trip_wayroute_details[['latitude', 'longitude']] = trip_wayroute_details[['latitude', 'longitude']].apply(pd.to_numeric, errors= 'coerce')
        return trip_wayroute_details
    
    def check4_trip_elimination(self, df2, trip_wayroute_details):        
            df2['TripID_EliminatedReason'] = 'Number of distinct GPS pings too low: % of total ping count < 20% ' 
            df2['TripID_EliminationKPI'] = round((len(trip_wayroute_details)/df2.loc[0,'TripID_rcrdCnt'])*100,2)
            df2['Waypoint_Cnt'] = 0
            return df2
        
    def hvrsne_distance_calc(self, from_lat_lng, to_lat_lng):
        return pd.Series(haversine_vector(from_lat_lng, to_lat_lng, Unit.KILOMETERS))
            
    def check5_trip_wayroute_details_dest_null(self, trip_wayroute_details):
        Dest_latLng = trip_wayroute_details[trip_wayroute_details['HvrsineDistOrgn_latlng'] == max(trip_wayroute_details['HvrsineDistOrgn_latlng'])].reset_index(drop= True)
        trip_wayroute_details['Dest_lat'] = Dest_latLng.loc[0,'latitude']
        trip_wayroute_details['Dest_lng'] = Dest_latLng.loc[0,'longitude']
        return trip_wayroute_details
        
    def check6_trip_elimination(self, df2, trip_wayroute_details):
        df2['TripID_EliminatedReason'] = 'Return trip GPS pings found: Distance between start lat-long and origin(Plant) lat-long:'
        df2['TripID_EliminationKPI'] = trip_wayroute_details.loc[0,'HvrsineDistOrgn_latlng']
        df2['Waypoint_Cnt'] = 0
        return df2
    
    def check7_trip_elimination(self, df2, trip_wayroute_details):
        df2['TripID_EliminatedReason'] = 'Destination GPS ping not found: min/max of destination to GPS pings distance >> 10% ' 
        df2['TripID_EliminationKPI'] = round(min(trip_wayroute_details.loc[:,'HvrsineDistDest_latlng'])/max(trip_wayroute_details.loc[:,'HvrsineDistDest_latlng'])*100,2)
        df2['Waypoint_Cnt'] = 0
        return df2
    
    def check8_trip_elimination(self, df2, trip_wayroute_details,v1):
        df2['TripID_EliminatedReason'] = 'Trip GPS ping start midway: Distance between Start GPS pings vs Src lat-long from FcltyMaster  >> 10% ' 
        df2['TripID_EliminationKPI'] = round((trip_wayroute_details.loc[0,'HvrsineDistOrgn_LatLng']/v1.iloc[0])*100,2)
        df2['Waypoint_Cnt'] = 0
        return df2
    
     
   
    def percentile_distance_origin_calc(self, trip_wayroute_details, prcnt): #function to calculate percentile distance from origin
        return np.percentile(trip_wayroute_details['HvrsineDistOrgn_latlng'], prcnt)
                        
    def percentile_distance_dest_calc(self, trip_wayroute_details, prcnt):
        return np.percentile(trip_wayroute_details['HvrsineDistDest_latlng'], prcnt)
    
    def trips_outside_range_func(self,trip_wayroute_details, Orgn_DistPrcnt85, Dest_DistPrcnt5):
        onward_trip_dest_LtLngGPS = trip_wayroute_details[(trip_wayroute_details['HvrsineDistOrgn_latlng'] > Orgn_DistPrcnt85) & (trip_wayroute_details['HvrsineDistDest_latlng'] < Dest_DistPrcnt5)]
        return onward_trip_dest_LtLngGPS
    
    def OnwardJourneyGPSData_func(self, trip_wayroute_details, present_truck_gps_attributes_raw, onward_trip_dest_LtLngGPS):
        OnwardJourneyGPSData = trip_wayroute_details[trip_wayroute_details.index <= min(onward_trip_dest_LtLngGPS.index)]     
        OnwardJourneyGPSData['latitude_4dec']=round(OnwardJourneyGPSData['latitude'],4)
        OnwardJourneyGPSData['longitude_4dec']=round(OnwardJourneyGPSData['longitude'],4)
        present_truck_gps_attributes = present_truck_gps_attributes_raw[['road','village','city','county','state_district','state','ISO3166-2-lvl4','postcode','latitude_4dec','longitude_4dec']].drop_duplicates().reset_index(drop= True)
        OnwardJourneyGPSData_1 = pd.merge(OnwardJourneyGPSData,present_truck_gps_attributes, how='left',on=['latitude_4dec','longitude_4dec'])
        return OnwardJourneyGPSData_1
    
    def gps_pings_func(self, OnwardJourneyGPSData_1):
        lat_long_req_hit = OnwardJourneyGPSData_1.loc[OnwardJourneyGPSData_1[['road', 'village', 'city', 'state_district', 'state', 'ISO3166-2-lvl4', 'postcode']].isnull().all(1)] #extracting specific lat-long data
        specific_trip_exist = OnwardJourneyGPSData_1[OnwardJourneyGPSData_1[['road', 'village', 'city', 'county', 'state_district', 'state', 'ISO3166-2-lvl4', 'postcode']].notnull().any(1)].drop_duplicates().reset_index(drop= True)
        gps_pings = lat_long_req_hit[['latitude_4dec', 'longitude_4dec']].drop_duplicates().reset_index(drop=True)
        return specific_trip_exist, gps_pings

    def specifc_trip_func(self, gps_pings, specific_trip_):
        specific_trip = pd.merge(gps_pings,specific_trip_[['latitude_4dec','longitude_4dec','ISO3166-2-lvl4']],on = ('latitude_4dec','longitude_4dec'),how='left') #gps_pings.copy() #pd.concat([specific_trip,gps_pings],ignore_index=True) 
        specific_trip = specific_trip[specific_trip['ISO3166-2-lvl4'].isnull()]
        specific_trip.drop(columns = ['ISO3166-2-lvl4'], inplace= True)
        specific_trip = specific_trip.drop_duplicates().reset_index(drop=True)
        return specific_trip    
    
    def check9_reverse_geolocator(self, master_df, specific_trip):
        for i in range(len(specific_trip)):
            lat1, lon1 = specific_trip.loc[i, "latitude_4dec"], specific_trip.loc[i, "longitude_4dec"]
            location1 = geolocator.reverse(f"{lat1},{lon1}", timeout=None).raw["address"]
            specific_trip.loc[i, "road"] = location1.get("road", specific_trip.loc[i, "road"])
            specific_trip.loc[i, "village"] = location1.get("village", specific_trip.loc[i, "village"])
            specific_trip.loc[i, "city"] = location1.get("city", specific_trip.loc[i, "city"])
            specific_trip.loc[i, "county"] = location1.get("county", specific_trip.loc[i, "county"])
            specific_trip.loc[i, "state_district"] = location1.get("state_district", specific_trip.loc[i, "state_district"])
            specific_trip.loc[i, "state"] = location1.get("state", specific_trip.loc[i, "state"])
            specific_trip.loc[i, "ISO3166-2-lvl4"] = location1.get("ISO3166-2-lvl4", specific_trip.loc[i, "ISO3166-2-lvl4"])
            specific_trip.loc[i, "postcode"] = location1.get("postcode", specific_trip.loc[i, "postcode"])

        master_df = pd.concat([master_df, specific_trip], axis=0, sort= False)        
        return specific_trip, master_df
        
    def way_OnwardJourneyGPSData_func(self, OnwardJourneyGPSData):
        strtGPS = OnwardJourneyGPSData.loc[0, 'HvrsineDistOrgn_latlng']
        strtWaypoint = OnwardJourneyGPSData.loc[0,'Waypoint']
        for i in range(len(OnwardJourneyGPSData)):
            if OnwardJourneyGPSData.loc[i, 'Waypoint'] == 'Null': OnwardJourneyGPSData.loc[i, 'NullValue'] = 1
            if OnwardJourneyGPSData.loc[i, 'HvrsineDistOrgn_latlng'] - strtGPS > 10: 
                OnwardJourneyGPSData.loc[i, 'Dist10km_Flag'] = 1
                strtGPS = OnwardJourneyGPSData.loc[i, 'HvrsineDistOrgn_latlng']
            if OnwardJourneyGPSData.loc[i, 'Waypoint'] != strtWaypoint:
                OnwardJourneyGPSData.loc[i, 'WaypointChng'] = 1
                strtWaypoint = OnwardJourneyGPSData.loc[i, 'Waypoint']
                
        return OnwardJourneyGPSData
    
    
    def Waypoint_Latlng_func(self, OnwardJourneyGPSData):
        StrtPoint_Lat = OnwardJourneyGPSData.loc[0,'latitude']
        StrtPoint_Lng = OnwardJourneyGPSData.loc[0,'longitude']
        Waypoint = OnwardJourneyGPSData.loc[0,'Waypoint']
        Waypoint_LatLng = pd.DataFrame([[1,StrtPoint_Lat,StrtPoint_Lng, Waypoint]],columns = ['SNo','Lat','Long','Waypoint'])
        i,k, NullValueFlag, Distncgrtr10Flag =1,1,0,0
        for i in range(len(OnwardJourneyGPSData)):
            if OnwardJourneyGPSData.loc[i, 'Dist10km_Flag'] == 1 : Distncgrtr10Flag = 1
            if OnwardJourneyGPSData.loc[i, 'WaypointChng'] == 1 & Distncgrtr10Flag == 1: 
                k = k+1
                Waypoint_LatLng = pd.concat([Waypoint_LatLng,pd.DataFrame([[k,OnwardJourneyGPSData.loc[i,'latitude'],OnwardJourneyGPSData.loc[i,'longitude'],OnwardJourneyGPSData.loc[i,'Waypoint']]],columns = ['SNo','Lat','Long','Waypoint'])],ignore_index=False)
                Distncgrtr10Flag= 0
                
        Waypoint_LatLng = pd.concat([Waypoint_LatLng,pd.DataFrame([[k+1,OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'latitude'],OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'longitude'],OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'Waypoint']]],columns = ['SNo','Lat','Long','Waypoint'])],ignore_index=False)
        #Lag lat-long values to find distance between two consecutive gps points
        Waypoint_LatLng['Key'] = 1
        Waypoint_LatLng['lag_lat'] = Waypoint_LatLng.groupby(['Key'])['Lat'].shift(1)
        Waypoint_LatLng['lag_lng'] = Waypoint_LatLng.groupby(['Key'])['Long'].shift(1)
        Waypoint_LatLng['lag_lat'] = Waypoint_LatLng['lag_lat'].fillna(Waypoint_LatLng['Lat'])
        Waypoint_LatLng['lag_lng'] = Waypoint_LatLng['lag_lng'].fillna(Waypoint_LatLng['Long'])
        
        Waypoint_LatLng.reset_index(inplace=True, drop= True)
        return Waypoint_LatLng
    
    def check10_trip_elimination(self, df2, Waypoint_LatLng):
        df2['HvrsineAggrtd'] = sum(Waypoint_LatLng['HvrsnDist_WayPointLatLng'])
        Waypoint_LatLng = Waypoint_LatLng[(Waypoint_LatLng['HvrsnDist_WayPointLatLng'] > 10) | (Waypoint_LatLng['HvrsnDist_WayPointLatLng'] == 0)]
        df2['TripID_EliminatedReason'] = '-' 
        df2['TripID_EliminationKPI'] = 0
        df2['Waypoint_Cnt'] = len(Waypoint_LatLng)
        return df2
    
    def assigned_Waypoint_LatLng(self, df2, Waypoint_LatLng):
        Waypoint_LatLng = Waypoint_LatLng.assign(TripID=df2.loc[0, 'Trip ID'], Plant=df2.loc[0, 'Plant'], CityCode=df2.loc[0, 'City Code'], depot_description=df2.loc[0, 'depot_description'], district_description=df2.loc[0, 'district_description'], taluka_descr=df2.loc[0, 'taluka_descr'], city_descr=df2.loc[0, 'city_descr'])
        return Waypoint_LatLng
    
    def find_LeadHvrsineMAPE_func(self, MD_Trip):
        MD_Trip['Lead_HvrsineMAPE'] = abs(MD_Trip['Lead'] - MD_Trip['HvrsineAggrtd'])/MD_Trip['Lead']
        return MD_Trip
    
    def unique_plant_city_combination(self, MD_Trip):
        UnqPlntCityCd_WypntMD = MD_Trip[['Plant','City Code']].drop_duplicates()
        UnqPlntCityCd_WypntMD.reset_index(inplace=True,drop=True)
        return UnqPlntCityCd_WypntMD
    
    def check11_selecting_routes(self, df, TripID_Waypoint_LatLng, UnqPlntCityCd_WypntMD, i2):
        WayPntCntMedian = df['Waypoint_Cnt'].median()
        chck = TripID_Waypoint_LatLng[(TripID_Waypoint_LatLng['Plant'] == UnqPlntCityCd_WypntMD.loc[i2,'Plant']) & (TripID_Waypoint_LatLng['City Code'] == UnqPlntCityCd_WypntMD.loc[i2,'City Code'])]
        scrChck = chck[['Waypoint','TripID']].drop_duplicates() 
        scrChck = scrChck.groupby(['Waypoint'],as_index=False)['TripID'].agg("count")
        scrChck['WaypointScore'] = scrChck['TripID']/len(MD_Trip[(MD_Trip['Plant'] == UnqPlntCityCd_WypntMD.loc[i2,'Plant']) & (MD_Trip['City Code'] == UnqPlntCityCd_WypntMD.loc[i2,'City Code'])])
        chck = pd.merge(chck, scrChck[['Waypoint','WaypointScore']],on='Waypoint',how='inner')
        scrChck = chck.groupby(['TripID'],as_index=False)['WaypointScore'].agg("sum")
        scrChck.rename(columns={'TripID':'Trip ID'},inplace=True)
        df = pd.merge(df,scrChck,on='Trip ID',how='inner')
        df['maxWypntscr'] = max(df['WaypointScore'])
        df = df[df['Lead_HvrsineMAPE'] == min(df['Lead_HvrsineMAPE'])]
        return df
    
    def check12_waypoint_LatLng_All_Routes_selected(self, TripID_Waypoint_LatLng, WWaypoint_MD_Trip_WminMAPE):
        Waypoint_LatLng_All_FnlRouteSlctd = pd.merge(TripID_Waypoint_LatLng,WWaypoint_MD_Trip_WminMAPE[['Trip ID','Waypoint_Cnt']],on='Trip ID',how='inner')
        Waypoint_LatLng_All_FnlRouteSlctd_All = pd.concat([Waypoint_LatLng_All_FnlRouteSlctd_All,Waypoint_LatLng_All_FnlRouteSlctd],ignore_index=True)
        return Waypoint_LatLng_All_FnlRouteSlctd_All

    
    
    def analysis(self, unique_plant_taluka_combination):
        column_names = ['latitude_4dec', 'longitude_4dec', 'road', 'village', 'city', 'county', 'state', 'ISO3166-2-lvl4', 'postcode']
        specific_trip = pd.DataFrame(columns= column_names)
        specific_trip_ = specific_trip.copy()
        MD_Trip = pd.DataFrame()
        MD_Trip_ = pd.DataFrame()
        master_df = specific_trip.copy()
        TripID_Waypoint_LatLng = pd.DataFrame()
        WWaypoint_MD_Trip = pd.DataFrame() 
        Waypoint_LatLng_All = pd.DataFrame()
        WWaypoint_MD_Trip_WminMAPE_ = pd.DataFrame()
        Waypoint_LatLng_All_FnlRouteSlctd = pd.DataFrame()
        Waypoint_LatLng_All_FnlRouteSlctd_All = pd.DataFrame()
        
        
        
        
        for ctr in range(len(unique_plant_taluka_combination)):
            #extract the gps attributes of the truck journey
            
            
            present_truck_gps_attributes_raw = pd.read_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\gps_attributes_jan.csv") 
            
            #filtering unique tripIDs based on conditions
            trip_list = self.trip_list_func(common_tripID_whole_Data, unique_plant_taluka_combination, ctr)
            
            if len(trip_list)>0: #check1 
                GPS_trip_location_details = self.fetch_GPS_data(trip_list)
            else:
                GPS_trip_location_details = pd.DataFrame()
            
            if len(GPS_trip_location_details) > 0: #check2
                dataj_ = self.check2_GPS_trip_location_details(GPS_trip_location_details, trip_list, dataj, unique_plant_taluka_combination, ctr)
                Axestrack_MergeGPS = self.Axestrack_MergeGPS_func(dataj_, GPS_trip_location_details)
                Axestrack_MergeGPS.to_csv('Axestrack_MergeGPS.csv')
                GPS_trip_location_details.to_csv('GPS_trip_location_details.csv')
                                
                count_trips_wrt_latlng = self.count_trips_latlng_func(GPS_trip_location_details)
                count_trips_wrt_latlng.to_csv('count_trips_wrt_latlng.csv')
                
                Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS, count_trips_wrt_latlng, on = 'Trip ID', how= 'inner')
                
                unique_trips_count = Axestrack_MergeGPS['Trip ID'].unique()
                total = 0
                # checkpoint3
                while (total < len(unique_trips_count)) & (len(unique_trips_count)>0):
                    df2 = self.check3_unique_trips_count(total, unique_trips_count, Axestrack_MergeGPS)
                    trip_wayroute_details = self.check3_trip_wayroute_details(total, unique_trips_count, Axestrack_MergeGPS)
                    # trip ID elimination
                    if (len(trip_wayroute_details)/df2.loc[0,'TripID_rcrdCnt'] < .2) | (len(trip_wayroute_details) < 2): #checkpoint4
                        df2 = self.check4_trip_elimination(df2, trip_wayroute_details)
                        MD_Trip = pd.concat([MD_Trip, df2], ignore_index= True)
                        
                    else:
                        from_lat_lng = trip_wayroute_details[['Orgn_lat', 'Orgn_lng']].to_numpy()
                        to_lat_lng = trip_wayroute_details[['latitude', 'longitude']].to_numpy()
                        trip_wayroute_details.loc[:, 'HvrsineDistOrgn_latlng'] = self.hvrsne_distance_calc(from_lat_lng, to_lat_lng)
                        
                        if len(trip_wayroute_details[trip_wayroute_details['Dest_lat'].isnull()]) > 0: #checkpoint5 if the destnation geog details are null
                            trip_wayroute_details = self.check5_trip_wayroute_details_dest_null(trip_wayroute_details)
                            
                        else:
                            pass
                    
                        from_lat_lng = trip_wayroute_details[['Dest_lat', 'Dest_lng']].to_numpy()
                        to_lat_lng = trip_wayroute_details[['latitude', 'longitude']].to_numpy()
                        trip_wayroute_details.loc[:, 'HvrsineDistDest_latlng'] = self.hvrsne_distance_calc(from_lat_lng, to_lat_lng)
                        
                        v1 = trip_wayroute_details[trip_wayroute_details['HvrsineDistDest_latlng'] == min(trip_wayroute_details.loc[:,'HvrsineDistDest_latlng'])]['HvrsineDistOrgn_latlng']

                        
                        if (trip_wayroute_details.loc[0,'HvrsineDistOrgn_latlng'] - trip_wayroute_details.loc[len(trip_wayroute_details)-1,'HvrsineDistOrgn_latlng'] > max(trip_wayroute_details.loc[:,'HvrsineDistOrgn_latlng'])*.5): #checkpoint 6
                            df2 = self.check6_trip_elimination(df2, trip_wayroute_details)
                            MD_Trip = pd.concat([MD_Trip, df2], ignore_index= True)
                            
                        else:
                            if (min(trip_wayroute_details.loc[:,'HvrsineDistDest_latlng'])/max(trip_wayroute_details.loc[:,'HvrsineDistDest_latlng']) > .1): #checkpoint 7
                                df2 = self.check7_trip_elimination(df2, trip_wayroute_details)
                                MD_Trip = pd.concat([MD_Trip, df2], ignore_index= True)
                                
                            else:
                                if (trip_wayroute_details.loc[0,'HvrsineDistOrgn_latlng']/v1.iloc[0] > .1): #checkpoint 8
                                    df2= self.check8_trip_elimination(df2, trip_wayroute_details)
                                    MD_Trip = pd.concat([MD_Trip, df2], ignore_index= True)
                                
                                else:
                                    Orgn_DistPrcnt85 = min(trip_wayroute_details.loc[0,'HvrsineDistDest_latlng'],self.percentile_distance_origin_calc(trip_wayroute_details,85))
                                    Dest_DistPrcnt5 = max(self.percentile_distance_dest_calc(trip_wayroute_details,5),.01)
                                    
                                    onward_trip_dest_LtLngGPS = self.trips_outside_range_func(trip_wayroute_details, Orgn_DistPrcnt85, Dest_DistPrcnt5)
                                    
                                    if (len(onward_trip_dest_LtLngGPS) == 0) : #checkpoint 9
                                        Dest_DistPrcnt5 = self.percentile_distance_dest_calc(trip_wayroute_details,5)
                                        onward_trip_dest_LtLngGPS = trip_wayroute_details[(trip_wayroute_details['HvrsineDistDest_latlng'] < Dest_DistPrcnt5)]
                                    
                                    OnwardJourneyGPSData_1 = self.OnwardJourneyGPSData_func(trip_wayroute_details, present_truck_gps_attributes_raw, onward_trip_dest_LtLngGPS)
                                    specific_trip_exist, gps_pings = self.gps_pings_func(OnwardJourneyGPSData_1)
                                   
                                    specific_trip = self.specific_trip_func(gps_pings, specific_trip_)
                                   
                                    if len(specific_trip)> 0: #check9 getting the lat-long details from the name of the place
                                       specific_trip, master_df = self.check9_reverse_geolocator(master_df, specific_trip)
                        
                                    specific_trip_ = pd.concat([specific_trip_,specific_trip],ignore_index=True)
                                    specific_trip_ = pd.concat([specific_trip_,specific_trip_exist],ignore_index=True)
                                    OnwardJourneyGPSData = pd.merge(OnwardJourneyGPSData,specific_trip_,how='inner',on=('latitude_4dec','longitude_4dec'))
                                    present_truck_gps_attributes_raw = present_truck_gps_attributes_raw.append(OnwardJourneyGPSData[['latitude', 'longitude', 'road', 'village', 'city', 'county','state_district', 'state', 'ISO3166-2-lvl4', 'postcode','latitude_4dec', 'longitude_4dec']]).drop_duplicates().reset_index(drop=True)
                                    
                                    if len(OnwardJourneyGPSData[OnwardJourneyGPSData['ISO3166-2-lvl4'].isnull()]) == 0: print('Good to Go')

                                    OnwardJourneyGPSData['Waypoint'] = OnwardJourneyGPSData['road'].combine_first(OnwardJourneyGPSData['village']).combine_first(OnwardJourneyGPSData['county']).combine_first(OnwardJourneyGPSData['city'])
                                    
                                    try:
                                        OnwardJourneyGPSData[['Waypoint', 'Waypoint2']] = OnwardJourneyGPSData['Waypoint'].str.split(';', 1, expand=True)
                                    except:
                                        pass
                                    
                                    OnwardJourneyGPSData = OnwardJourneyGPSData.assign(Dist10km_Flag=0, WaypointChng=0, NullValue=0, Waypoint=OnwardJourneyGPSData['Waypoint'].fillna('Null'))
                                    
                                    OnwardJourneyGPSData = self.way_OnwardJourneyGPSData_func(OnwardJourneyGPSData)
                                    
                                    Waypoint_Latlng = self.Waypoint_Latlng_func(OnwardJourneyGPSData)
                                    
                                    from_lat_lng = Waypoint_Latlng[['lag_lat', 'lag_lng']].to_numpy()
                                    to_lat_lng = Waypoint_Latlng[['Lat', 'Long']].to_numpy()

                                    Waypoint_LatLng.loc[:,'HvrsnDist_WayPointLatLng'] = self.hvrsne_distance_calc(from_lat_lng, to_lat_lng)
                                    
                                    df2 = self.check10_trip_elimination(df2, Waypoint_LatLng) #checkpoint 10 for trip elimination
                                    
                                    Waypoint_LatLng = self.assigned_Waypoint_LatLng(df2, Waypoint_LatLng)
                                    
                                    TripID_Waypoint_LatLng = pd.concat([TripID_Waypoint_LatLng, Waypoint_LatLng], ignore_index=True).reset_index(drop=True)
                                    MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)

                total += 1
                
            MD_Trip_ = pd.concat([MD_Trip_, MD_Trip], ignore_index= True)
            MD_Trip = MD_Trip[MD_Trip['Waypoint_Cnt'] > 0].reset_index(drop = True)
            WWaypoint_MD_Trip = pd.concat([WWaypoint_MD_Trip,MD_Trip],ignore_index=True)
            Waypoint_LatLng_All = pd.concat([Waypoint_LatLng_All,TripID_Waypoint_LatLng],ignore_index=True)
            MD_Trip = self.find_LeadHvrsineMAPE_func(MD_Trip) #finding HvrsineMAPE by subtracting HvrsineAggrtd from Lead and dividing by Lead
            UnqPlntCityCd_WypntMD = self.unique_plant_city_combination(MD_Trip)
            for j in range(len(UnqPlntCityCd_WypntMD)): #selecting the best route for a plant-city combination out of the recorded trip IDs
                df = MD_Trip[(MD_Trip['Plant'] == UnqPlntCityCd_WypntMD.loc[j,'Plant']) & (MD_Trip['City Code'] == UnqPlntCityCd_WypntMD.loc[j,'City Code'])]
                if len(df) > 1: #checkpoint 11
                    df = self.check11_selecting_routes(df, TripID_Waypoint_LatLng, UnqPlntCityCd_WypntMD)
                    if len(df) > 1:
                        df = df[df['Waypoint_Cnt'] == max(df['WaypointScore'])]
                
                WWaypoint_MD_Trip_WminMAPE = pd.concat([WWaypoint_MD_Trip_WminMAPE,df],ignore_index=True)
            
            WWaypoint_MD_Trip_WminMAPE.reset_index(inplace=True, drop = True)              
            WWaypoint_MD_Trip_WminMAPE_ = pd.concat([WWaypoint_MD_Trip_WminMAPE_,WWaypoint_MD_Trip_WminMAPE],ignore_index=True)

            TripID_Waypoint_LatLng.rename(columns={'TripID':'Trip ID'},inplace=True)
            
            if len(WWaypoint_MD_Trip_WminMAPE) > 0: #checkpoint 12
                Waypoint_LatLng_All_FnlRouteSlctd_All = self.check12_waypoint_LatLng_All_Routes_selected(TripID_Waypoint_LatLng, WWaypoint_MD_Trip_WminMAPE)
            
        MD_Trip_.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\MD_Trip_jan.csv",index=False)
        WWaypoint_MD_Trip_WminMAPE_.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\waypoint_summary_jan.csv",index=False)
        Waypoint_LatLng_All_FnlRouteSlctd_All.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\waypoint_trip_id_level_jan.csv",index=False)
        present_truck_gps_attributes_raw.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\gps_attributes_jan.csv",index=False)

    
        return MD_Trip_,WWaypoint_MD_Trip_WminMAPE_,Waypoint_LatLng_All_FnlRouteSlctd_All,present_truck_gps_attributes_raw
                
                
                
if __name__ == '__main__':

    MonthRun = "jan23" #generate waypoint records for the month
    MonitoredTripGPSData_Input_mypath = "C:\\Users\\nikhil.upadhyay-v\\Downloads\\SED\\" + MonthRun + "\\"

    #setting the configuration file    
    config = ConfigParser()  
    config.read(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\config.ini") 

    # Production Credentials
    host_name_prod = config['Cred_Prod']['host']
    database_name_prod = config['Cred_Prod']['dbname']
    user_name_prod = config['Cred_Prod']['user']
    user_password_prod = config['Cred_Prod']['password']
    port_prod =config['Cred_Prod']['port']
    
    # Dev Credentials
    host_name_dev = config['Cred_Dev']['host']
    database_name_dev = config['Cred_Dev']['dbname']
    user_name_dev = config['Cred_Dev']['user']
    user_password_dev = config['Cred_Dev']['password']
    port_dev =config['Cred_Dev']['port']

    geolocator = Nominatim(user_agent="geoapiExercises")
    
    conn = connection_to_database(database_name_prod,host_name_prod,port_prod,user_name_prod, user_password_prod) #establishing connection to the database
    cur = conn.cursor() #starting the cursor
    
    data_fetcher = loadData(conn) #object to the class loadData
    
    #sql commands to fetch the data    
    sql_facility_master = """select t.* from (Select fm1.* from dna_ultratech_fare_dev.node_facility_master_stg as fm1 ) as t;"""
    
    sql_node_facility_master = """select t.* from (Select fm1.* from dna_ultratech_fare_dev.node_facility as fm1 ) as t;"""
    
    sql_customer_master = """select t.* from (Select fm1.* from dna_ultratech_fare_dev.node_demand_region_stg as fm1 ) as t;"""
    
    sql_node_customer_master = """select t.* from (Select fm1.* from dna_ultratech_fare_dev.node_customer as fm1 ) as t;"""
    
    sql_node_address = """select t.* from (Select fm1.* from dna_ultratech_fare_dev.node_address as fm1 ) as t;"""
    
    sql_unique_tripID_GPS = """select distinct(trip_id) from dna_ultratech_fare_dev.vw_gps_location_details_monitored_trip_jan"""
    
    sql_fetch_GPS = """select * from dna_ultratech_fare_dev.vw_gps_location_details_monitored_trip_jan where trip_id in %(l)s"""
    
    #calling the function fetch_data through the object
    facility_master, node_facility_master, customer_master, node_customer_master, node_address = data_fetcher.fetch_data(sql_facility_master), data_fetcher.fetch_data(sql_node_facility_master), data_fetcher.fetch_data(sql_customer_master),    data_fetcher.fetch_data(sql_node_customer_master), data_fetcher.fetch_data(sql_node_address)
    
    facility_master.to_csv('facility_master.csv')
    node_facility_master.to_csv('node_facility_master.csv')
    customer_master.to_csv('customer_master.csv')
    node_customer_master.to_csv('node_customer_master.csv')
    node_address.to_csv('node_address.csv')
    
    #-----------------------------------------------------------------------------------------------------
    dp = dataProcessing(facility_master, node_facility_master, customer_master, node_customer_master, node_address) # object of the second class dataProcessing
    Waypoint_WCustFclty_WLatLng = dp.dataPreparation()
    Output = dp.importing_file(MonitoredTripGPSData_Input_mypath)
    Output.to_csv('Output.csv')
    dataj = dp.importing_monitored_trip_data(MonthRun, MonitoredTripGPSData_Input_mypath)
    dataj.to_csv('dataj.csv')    
    #------------------------------------------------------------------------------------------------------    
    
    unique_tripID_GPS_data = Unique_list_tripid_GPS()
    unique_tripID_GPS_data.rename(columns= {'trip_id': 'Trip ID'}, inplace= True)
    unique_tripID_GPS_data.to_csv('unique_tripID_GPS_data.csv')
    
    unique_tripID_monitored = dataj.loc[dataj['Lead_SAP'] > 40] 
    unique_tripID_monitored = unique_tripID_monitored[['Trip ID']].drop_duplicates().reset_index(drop=True)
    unique_tripID_monitored['Trip ID']= unique_tripID_monitored['Trip ID'].astype('str')
    unique_tripID_monitored.to_csv('unique_tripID_monitored.csv')

    #------------------------------------------------------------------------------------------------------
    wp = waypoint(conn, dataj, unique_tripID_monitored, unique_tripID_GPS_data, Waypoint_WCustFclty_WLatLng, geolocator) #object of the third class waypoint
    
    
    common_tripID = wp.common_tripID_func()
    common_tripID.to_csv('common_tripID.csv')    
    common_tripID_whole_Data, unique_plant_taluka_combination = wp.unique_plant_taluka_combination_func(common_tripID)
    common_tripID_whole_Data.to_csv('common_tripID_whole_Data.csv')
    unique_plant_taluka_combination.to_csv('unique_plant_taluka_combination.csv')

    #***** 
    
    
    MD_Trip_,WWaypoint_MD_Trip_WminMAPE_,Waypoint_LatLng_All_FnlRouteSlctd_All,present_truck_gps_attributes_raw = wp.analysis(unique_plant_taluka_combination)

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
