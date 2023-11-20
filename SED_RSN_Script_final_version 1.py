import numpy as np
import pandas as pd
from haversine import haversine_vector, Unit
import psycopg2
from configparser import ConfigParser
import pandas.io.sql as psql
from os import listdir
from os.path import isfile, join
import re
from geopy.geocoders import Nominatim
import os
import pdb
print("initiliazing")



def database_connection(database_name,host_name,port,user_name,user_password):
    connection =psycopg2.connect(dbname=database_name,
                           host=host_name,
                           port=port,
                           user=user_name,
                           password=user_password)
    
    return connection


def fetching_data(cur):
    cursor = cur.cursor()
    # cursor.close()

    # SQL query to fetch the node data from FACILITY MASTER
    sql = """select t.* from 
    (Select fm1.* from dna_ultratech_fare_dev.node_facility_master_stg as fm1 ) as t
    ;"""

    cursor.execute(sql)
    col = [desc[0] for desc in cursor.description]
    FACILITY_MASTER = cursor.fetchall()
    FACILITY_MASTER = pd.DataFrame(FACILITY_MASTER, columns=col)
    if len(FACILITY_MASTER) > 0:
        print('Good to Go')
    facility_master = FACILITY_MASTER
    if len(facility_master[['facility_code']].drop_duplicates()) == len(facility_master):
        print('Good to Go')


    sql = """select t.* from 
    (Select fm1.* from dna_ultratech_fare_dev.node_facility as fm1 ) as t
    ;"""

    cursor.execute(sql)
    col = [desc[0] for desc in cursor.description]
    NODE_FACILITY_MASTER = cursor.fetchall()
    NODE_FACILITY_MASTER = pd.DataFrame(NODE_FACILITY_MASTER, columns=col)
    if len(NODE_FACILITY_MASTER) > 0:
        print('Good to Go')
    node_facility_master = NODE_FACILITY_MASTER
    # Check for duplicates at CITY_CODE
    if len(node_facility_master[['address_id_id']].drop_duplicates()) == len(node_facility_master):
        print('Good to Go')



    sql = """select t.* from 
    (Select fm1.* from dna_ultratech_fare_dev.node_demand_region_stg as fm1 ) as t
    ;"""

    cursor.execute(sql)
    col = [desc[0] for desc in cursor.description]
    CUSTOMER_MASTER = cursor.fetchall()
    CUSTOMER_MASTER = pd.DataFrame(CUSTOMER_MASTER, columns=col)
    if len(CUSTOMER_MASTER) > 0:
        print('Good to Go')
    customer_master = CUSTOMER_MASTER
    # Check for duplicates at CITY_CODE
    if len(customer_master[['city_code']].drop_duplicates()) == len(customer_master):
        print('Good to Go')


    sql = """select t.* from 
    (Select fm1.* from dna_ultratech_fare_dev.node_customer as fm1 ) as t
    ;"""

    cursor.execute(sql)
    col = [desc[0] for desc in cursor.description]
    NODE_CUSTOMER_MASTER = cursor.fetchall()
    NODE_CUSTOMER_MASTER = pd.DataFrame(NODE_CUSTOMER_MASTER, columns=col)
    if len(NODE_CUSTOMER_MASTER) > 0:
        print('Good to Go')
    node_customer_master = NODE_CUSTOMER_MASTER
    # Check for duplicates at CITY_CODE
    if len(node_customer_master[['address_id_id']].drop_duplicates()) == len(node_customer_master):
        print('Good to Go')


    sql = """select t.* from 
    (Select fm1.* from dna_ultratech_fare_dev.node_address as fm1 ) as t
    ;"""

    cursor.execute(sql)
    col = [desc[0] for desc in cursor.description]
    NODE_ADDRESS = cursor.fetchall()
    NODE_ADDRESS = pd.DataFrame(NODE_ADDRESS, columns=col)
    if len(NODE_ADDRESS) > 0:
        print('Good to Go')
    node_address = NODE_ADDRESS
    # Check for duplicates at CITY_CODE
    if len(node_address[['id']].drop_duplicates()) == len(node_address):
        print('Good to Go')
        
    return facility_master,node_facility_master,customer_master,node_customer_master,node_address


def fetching_lat_long_cust_facility(facility_master,node_facility_master,customer_master,node_customer_master,node_address):
    Facility_Master = pd.merge(node_facility_master[['facility', 'facility_code', 'facility_desc', 'facility_processing', 'facility_sub_type', 'validated_by', 'i2_code', 'address_id_id']],
                               node_address[['id', 'lat', 'lng']], how='left', left_on='address_id_id', right_on='id')
    del Facility_Master['address_id_id']
    del Facility_Master['id']
    Facility_Master = pd.merge(Facility_Master, facility_master[[
                               'facility_code', 'depot_code', 'depot_desc']], how='left', on='facility_code')

    Cust_Master = pd.merge(node_customer_master[['customer_code', 'is_active', 'validated_by', 'address_id_id']], node_address[[
                           'id', 'lat', 'lng']], how='left', left_on='address_id_id', right_on='id')
    del Cust_Master['address_id_id']
    del Cust_Master['id']

    Cust_Master = pd.merge(Cust_Master,
                           customer_master[['city_code', 'city_descr', 'taluka_code', 'taluka_descr', 'district_code', 'district_description', 'depot_code', 'depot_description', 'region_code', 'region_description', 'state_code', 'state_description', 'zone_code', 'zone_descr', 'country', 'active']], how='left', left_on='customer_code', right_on='city_code')
    del Cust_Master['is_active']

    Cust_Master.rename(columns={'city_descr': 'CITY_DESCR',
                       'depot_description': 'DEPOT_DESCRIPTION'}, inplace=True)

    # Rename DEPOT_DESC, FACILITY_DESCRIPTION & FACILITY_PROCESSING in Facility_master
    Facility_Master.rename(columns={'depot_desc': 'SLOC_DEPOT', 'facility_desc': 'SLOC_DESC',
                           'facility_processing': 'WH_TYPE', 'i2_code': 'FACILITY_KEY'}, inplace=True)

    CustLtLng = Cust_Master[['DEPOT_DESCRIPTION','district_description','taluka_descr','CITY_DESCR','customer_code','lat','lng']]
    CustLtLng['Identifier'] = 'CUSTOMER'
    CustLtLng.rename(columns={'CITY_DESCR':'DESCR','customer_code':'KEY'},inplace=True)
    FcltyLtLng = Facility_Master[['SLOC_DEPOT','SLOC_DESC','facility_code','lat','lng','WH_TYPE']]
    FcltyLtLng.rename(columns={'SLOC_DESC':'DESCR','facility_code':'KEY','WH_TYPE':'Identifier'},inplace=True)
    FcltyLtLng['DEPOT_DESCRIPTION'] = FcltyLtLng['DESCR']
    FcltyLtLng['district_description'] = FcltyLtLng['DESCR']
    FcltyLtLng['taluka_descr'] = FcltyLtLng['DESCR']
    Waypoint_WCustFclty_WLatLng = pd.concat([FcltyLtLng,CustLtLng],ignore_index=True).drop_duplicates()
    return Waypoint_WCustFclty_WLatLng


def importing_monitored_trip_data(MonthRun,MonitoredTripGPSData_Input_mypath):
    MonitoredTrip_Axestrack_LtstMnth = pd.DataFrame()
   
    # Folder path containing the files
    # folder_path = MonitoredTripGPSData_Input_mypath

    # # Initialize an empty DataFrame to store the combined data
    # combined_data = pd.DataFrame()
    # print(os.listdir(folder_path))
    # # Iterate over all files in the folder
    # for file_name in os.listdir(folder_path):
    #     file_path = os.path.join(folder_path, file_name)
        
    #     # Check if the file is in XLSX or CSV format
    #     if file_name.endswith(".xlsx"):
    #         data = pd.read_excel(file_path)
    #     elif file_name.endswith(".csv"):
    #         data = pd.read_csv(file_path)
    #     else:
    #         continue  # Skip files that are not in XLSX or CSV format
        
    #     # Append the data to the combined_data DataFrame
    #     combined_data = combined_data.append(data, ignore_index=True)

    onlyfiles = pd.DataFrame([f for f in listdir(MonitoredTripGPSData_Input_mypath) if isfile(join(MonitoredTripGPSData_Input_mypath, f))])
    onlyfiles.columns = ['FName']

    #Import Google API hit results 
    FName = onlyfiles[(onlyfiles['FName'].str.contains(".xlsx")) | (onlyfiles['FName'].str.contains('.csv'))]
    FName.reset_index(inplace=True)
    del FName['index']
    InputFile = MonitoredTripGPSData_Input_mypath + FName['FName'].values[0]
    if  len(re.compile(r'\b' + r'\S*' + re.escape('.xlsx') + r'\S*').findall(InputFile)) > 0: Output = pd.read_excel(InputFile, thousands=",",sheet_name = 'Consignment Report',engine='openpyxl')
    if  len(re.compile(r'\b' + r'\S*' + re.escape('.csv') + r'\S*').findall(InputFile)) > 0: Output = pd.read_csv(InputFile, thousands=",",sheet_name = 'Consignment Report')
    i = 1
    while i < len(FName):
        InputFile = MonitoredTripGPSData_Input_mypath + FName['FName'].values[i]
        #File import from the path + folder
        if  len(re.compile(r'\b' + r'\S*' + re.escape('.xlsx') + r'\S*').findall(InputFile)) > 0: Output = pd.concat([Output,pd.read_excel(InputFile, thousands=",",sheet_name = 'Consignment Report',engine='openpyxl')],ignore_index=True)
        if  len(re.compile(r'\b' + r'\S*' + re.escape('.csv') + r'\S*').findall(InputFile)) > 0: Output = pd.concat([Output,pd.read_csv(InputFile, thousands=",",sheet_name = 'Consignment Report')],ignore_index=True)
        i = i+1
    
    MonitoredTrip_Axestrack = Output.copy() 
    
    MonitoredTrip_Axestrack.rename(str.strip, axis='columns', inplace=True)
    MonitoredTrip_Axestrack['Month']  = MonthRun
    MonitoredTrip_Axestrack['Plant'] = MonitoredTrip_Axestrack['Plant Code'].astype('str')
    MonitoredTrip_Axestrack['Plant Code'] = MonitoredTrip_Axestrack['Plant Code'].astype('str')
    MonitoredTrip_Axestrack['Plant Code'] = MonitoredTrip_Axestrack['Plant Code'].str.slice(0, 4)
    MonitoredTrip_Axestrack.columns
    MonitoredTrip_Axestrack = MonitoredTrip_Axestrack[(MonitoredTrip_Axestrack['Accountability'] == 'Monitored')|(MonitoredTrip_Axestrack['Trip Status'] == 'TRIP MONITORED')][['Month','Trip ID','Plant Code','Depot','City Code','DI No','SAP Lead Distance (Kms)','Plant']]
    MonitoredTrip_Axestrack.info()
    MonitoredTrip_Axestrack.rename(columns={'SAP Lead Distance (Kms)':'Lead_SAP'},inplace=True)
    MonitoredTrip_Axestrack['Trip ID'] = MonitoredTrip_Axestrack['Trip ID'].astype('int64')
    MonitoredTrip_Axestrack['DI No'] = MonitoredTrip_Axestrack['DI No'].astype('int64')

    MonitoredTrip_Axestrack.reset_index(inplace=True)
    del MonitoredTrip_Axestrack['index']
    
    MonitoredTrip_Axestrack_LtstMnth = pd.concat([MonitoredTrip_Axestrack_LtstMnth,MonitoredTrip_Axestrack],ignore_index=True)

    MonitoredTrip_Axestrack_LtstMnth['trip_id'] = MonitoredTrip_Axestrack_LtstMnth['Trip ID'].astype('str')
    
    #Merge with Cust/Facility to fetch depot-district-taluka-city descr corresponding to each city code
    MonitoredTrip_Axestrack_LtstMnth = pd.merge(MonitoredTrip_Axestrack_LtstMnth, customer_master[['city_code','depot_description','district_description','taluka_descr','city_descr','state_description']],how='left',left_on='City Code',right_on = 'city_code')
    
    #Remove Plant code 'SD01' from monitored trip data
    Axestrack_MonitoredTrip = MonitoredTrip_Axestrack_LtstMnth[(MonitoredTrip_Axestrack_LtstMnth['Plant Code'] != 'SD01') & (MonitoredTrip_Axestrack_LtstMnth['Plant Code'] != 'KC01') & (MonitoredTrip_Axestrack_LtstMnth['Plant Code'] != 'GK01') & (MonitoredTrip_Axestrack_LtstMnth['Plant Code'] != 'BA01')]

    #Unique Plant city combination from monitored trip data
    Axestrack_MonitoredTrip['Plant_City'] = Axestrack_MonitoredTrip['Plant Code'] + Axestrack_MonitoredTrip['City Code']
    Axestrack_MonitoredTrip['Plant_Depot'] = Axestrack_MonitoredTrip['Plant Code'] + Axestrack_MonitoredTrip['depot_description']
    Axestrack_MonitoredTrip['Plant_District'] = Axestrack_MonitoredTrip['Plant Code'] + Axestrack_MonitoredTrip['district_description']
    Axestrack_MonitoredTrip['Plant_Taluka'] = Axestrack_MonitoredTrip['Plant Code'] + Axestrack_MonitoredTrip['taluka_descr']
    Axestrack_MonitoredTrip['Plant_CityDescr'] = Axestrack_MonitoredTrip['Plant Code'] + Axestrack_MonitoredTrip['city_descr']
    Unq_PlantTaluka_RcrdCnt = Axestrack_MonitoredTrip.groupby(['Plant Code','taluka_descr'],as_index=False)['trip_id'].agg("count").sort_values(['trip_id']).reset_index(drop=True)

    #Filter out all the trip IDs corresponding to one plant-city code combination from the Depot-District-Taluka
    dataj = Axestrack_MonitoredTrip[Axestrack_MonitoredTrip['Plant_Depot'] != 'x']
    # dataj.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output\dataj_output.csv')
    return dataj

def Unique_list_tripid_GPS():
    cur = database_connection(database_name_dev,host_name_dev,port_dev,user_name_dev,user_password_dev)
    cursor = cur.cursor()
    cursor.execute('select distinct(trip_id) from dna_ultratech_fare_dev.vw_gps_location_details_monitered_trip_jan')    
    col = [desc[0] for desc in cursor.description]
    unique_trip_id_in_GPS_data = cursor.fetchall()
    unique_trip_id_in_GPS_data = pd.DataFrame(unique_trip_id_in_GPS_data, columns=col)
    return unique_trip_id_in_GPS_data


def fetching_gps_data(unqiue_TID):
    cur = database_connection(database_name_dev,host_name_dev,port_dev,user_name_dev,user_password_dev)
    cursor = cur.cursor()
    #Fetch GPS ping data from DB for all the unique TIDs
    l = unqiue_TID
    l = tuple(l)
    
    params = {'l': l}
    
    cursor.execute('select * from dna_ultratech_fare_dev.vw_gps_location_details_monitered_trip_jan where trip_id in %(l)s',params)    
    
    col = [desc[0] for desc in cursor.description]
    GPS_locdtls_TID = cursor.fetchall()
    GPS_locdtls_TID = pd.DataFrame(GPS_locdtls_TID, columns=col)
    return GPS_locdtls_TID
    
def way_point_analysis(dataj,unique_trip_id_Monitored,unique_trip_id_in_GPS_data,Waypoint_WCustFclty_WLatLng):
    unique_trip_id_in_GPS_data_=unique_trip_id_in_GPS_data[unique_trip_id_in_GPS_data['trip_id'].notnull()]
    unique_trip_id_Monitored['Trip ID']=unique_trip_id_Monitored['Trip ID'].astype(str)
    
    common_trip_id_ = unique_trip_id_in_GPS_data_.loc[unique_trip_id_in_GPS_data_['trip_id'].isin(set(unique_trip_id_Monitored['Trip ID']))].reset_index(drop=True)
    common_trip_id_whole_Data = dataj.loc[dataj['trip_id'].isin(set(common_trip_id_['trip_id']))].reset_index(drop=True)
    
    Unq_PlantTaluka_RcrdCnt = common_trip_id_whole_Data.groupby(['Plant Code','taluka_descr'],as_index=False)['trip_id'].agg("count").sort_values(['trip_id']).reset_index(drop=True)
    
    Unq_PlantTaluka_RcrdCnt=Unq_PlantTaluka_RcrdCnt.reset_index(drop=True)
    
    specific_trip = pd.DataFrame()
    
    #Code to fetch details of each lat-long using geocoder package
    specific_trip = pd.DataFrame() #gps_pings.copy() #pd.concat([specific_trip,gps_pings],ignore_index=True) 
    specific_trip["latitude_4dec"] = np.nan
    specific_trip["longitude_4dec"] = np.nan
    
    
    specific_trip["road"] = np.nan
    specific_trip["village"] = np.nan
    specific_trip["city"] = np.nan
    specific_trip["county"] = np.nan
    specific_trip["state_district"] = np.nan
    specific_trip["state"] = np.nan
    specific_trip["ISO3166-2-lvl4"] = np.nan
    specific_trip["postcode"] = np.nan

    specific_trip_ = specific_trip.copy()
    master_df = specific_trip.copy()

    MonitoredTrip_WIncorrctData_ = pd.DataFrame()
    MD_Trip = pd.DataFrame()
    MD_Trip_ = pd.DataFrame()
    
    TripID_Waypoint_LatLng = pd.DataFrame() 
    WWaypoint_MD_Trip = pd.DataFrame() 
    Waypoint_LatLng_All = pd.DataFrame()
    WWaypoint_MD_Trip_WminMAPE_ = pd.DataFrame()
    
    Waypoint_LatLng_All_FnlRouteSlctd_All = pd.DataFrame()
    
    # print(Unq_PlantTaluka_RcrdCnt.head())
    # len(Unq_PlantTaluka_RcrdCnt)
    # pdb.set_trace()
    # Unq_PlantTaluka_RcrdCnt.to_csv('/home/viplav/JAN_Output/unique_list_of_plant_taluka_jan.csv')
    cntr1=0
    while cntr1 < len(Unq_PlantTaluka_RcrdCnt):
        # MD_Trip_ = pd.read_csv('/home/viplav/JAN_Output/MD_trip_jan.csv')
        # WWaypoint_MD_Trip_WminMAPE_ = pd.read_csv('/home/viplav/JAN_Output/waypoint_summary_jan.csv')
        # Waypoint_LatLng_All_FnlRouteSlctd_All = pd.read_csv('/home/viplav/JAN_Output/waypoint_trip_id_level_jan.csv')
        present_truck_gps_attributes_raw=pd.read_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\gps_attributes_jan.csv")
        present_truck_gps_attributes_raw.head()
        print(cntr1)
        print(Unq_PlantTaluka_RcrdCnt['Plant Code'][cntr1])
        print(Unq_PlantTaluka_RcrdCnt['taluka_descr'][cntr1])
        unqiue_TID =  common_trip_id_whole_Data.loc[(common_trip_id_whole_Data['Plant Code']==Unq_PlantTaluka_RcrdCnt['Plant Code'][cntr1]) & (common_trip_id_whole_Data['taluka_descr']==Unq_PlantTaluka_RcrdCnt['taluka_descr'][cntr1]) & (common_trip_id_whole_Data['Lead_SAP'] > 40)]['trip_id'].unique()

        if len(unqiue_TID) > 0:
            GPS_locdtls_TID=fetching_gps_data(unqiue_TID) 
            # GPS_locdtls_TID.to_csv('/home/viplav/JAN_Output/GPS_locdtls_TID.csv')
        else: 
            GPS_locdtls_TID = pd.DataFrame()
        
        if len(GPS_locdtls_TID) > 0:
            unqiue_TID=unqiue_TID.tostring()
            dataj_ = dataj[(dataj['Plant Code']==Unq_PlantTaluka_RcrdCnt.loc[cntr1,'Plant Code']) & (dataj['taluka_descr']==Unq_PlantTaluka_RcrdCnt.loc[cntr1,'taluka_descr'])]          

            dataj_ = dataj_.sort_values(by=['Lead_SAP'],ascending = False)
            dataj_ = dataj_.drop_duplicates(subset=['trip_id'],keep='first').reset_index(drop=True)

            Axestrack_MergeGPS = pd.merge(dataj_,GPS_locdtls_TID,on = 'trip_id',how='inner')
            
            #Fetch Lat-Long corresponding to orgn(Plant)-destination(CityCode) from Facility-Cusomer Master
            Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS,Waypoint_WCustFclty_WLatLng[['KEY','lat','lng']],left_on='Plant Code',right_on='KEY',how='left')
            
            
            del Axestrack_MergeGPS['KEY']
            Axestrack_MergeGPS.rename(columns={'lat':'Orgn_lat','lng':'Orgn_lng'},inplace = True)
            
            Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS,Waypoint_WCustFclty_WLatLng[['KEY','lat','lng']],left_on='Plant',right_on='KEY',how='left')
            del Axestrack_MergeGPS['KEY']
            Axestrack_MergeGPS['Orgn_lat'] = Axestrack_MergeGPS['Orgn_lat'].fillna(Axestrack_MergeGPS['lat']) 
            Axestrack_MergeGPS['Orgn_lng'] = Axestrack_MergeGPS['Orgn_lng'].fillna(Axestrack_MergeGPS['lng']) 
            del Axestrack_MergeGPS['lat']
            del Axestrack_MergeGPS['lng']
            
            Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS,Waypoint_WCustFclty_WLatLng[['KEY','lat','lng']],left_on='City Code',right_on='KEY',how='left')
            del Axestrack_MergeGPS['KEY']
            Axestrack_MergeGPS.rename(columns={'lat':'Dest_lat','lng':'Dest_lng'},inplace = True)
            
            Axestrack_MergeGPS['DtTimeSeq'] = pd.to_datetime(Axestrack_MergeGPS['lat_long_time'], format='%d%m%Y%H%M%S')
            
            # Check 1
            
            # MonitoredTrip_WIncorrctData1 = Axestrack_MergeGPS[['trip_id','latitude','longitude']].drop_duplicates().groupby(['trip_id'],as_index=False)['latitude','longitude'].agg("count")
            
            # MonitoredTrip_WIncorrctData2 = Axestrack_MergeGPS.groupby(['trip_id'],as_index=False)['lat_long_time'].agg("count")
            
            # MonitoredTrip_WIncorrctData = pd.merge(MonitoredTrip_WIncorrctData1,MonitoredTrip_WIncorrctData2,how='inner',on='trip_id') #GPS_locdtls_TID[GPS_locdtls_TID['trip_id'] == '240810221745772']
            # MonitoredTrip_WIncorrctData['Prcnt_Off'] = (MonitoredTrip_WIncorrctData['lat_long_time'] - MonitoredTrip_WIncorrctData['latitude'])/MonitoredTrip_WIncorrctData['lat_long_time']
            # MonitoredTrip_WIncorrctData =MonitoredTrip_WIncorrctData[MonitoredTrip_WIncorrctData['Prcnt_Off'] > .95]
            # MonitoredTrip_WIncorrctData_=pd.DataFrame()
            # MonitoredTrip_WIncorrctData_ = pd.concat([MonitoredTrip_WIncorrctData_,MonitoredTrip_WIncorrctData],ignore_index=True)
            
            # after_check_1_GPS_data = Axestrack_MergeGPS.loc[~(Axestrack_MergeGPS['Trip ID'].isin([MonitoredTrip_WIncorrctData_['trip_id']]))]
            unique_TripIDs = Axestrack_MergeGPS['Trip ID'].unique()
            
            TripID_CntTotLatLng = GPS_locdtls_TID.groupby(['trip_id'],as_index=False)['lat_long_time'].agg("count")
            TripID_CntTotLatLng.rename(columns={'lat_long_time':'TotGPSPingsCnt_perTripID'},inplace=True)
            Axestrack_MergeGPS = pd.merge(Axestrack_MergeGPS,TripID_CntTotLatLng,how='inner',on='trip_id')
            
            MD_Trip = pd.DataFrame()
            TripID_Waypoint_LatLng = pd.DataFrame() 
            unique_TripIDs = Axestrack_MergeGPS['Trip ID'].unique() #TripID_WRouteDetails[(TripID_WRouteDetails['Plant Code'] == '6970') & (TripID_WRouteDetails['City Code'] == 'ODC2')]['Trip ID'].unique()
            TIDCnt=0
            while (TIDCnt < len(unique_TripIDs)) & (len(unique_TripIDs)>0):
                df = Axestrack_MergeGPS[Axestrack_MergeGPS['Trip ID'] == unique_TripIDs[TIDCnt]]
                df.reset_index(inplace=True, drop = True)
                df2 = pd.DataFrame([[1,df.loc[0,'Trip ID'],df.loc[0,'Plant Code'],df.loc[0,'City Code'],df.loc[0,'Lead_SAP'],df.loc[0,'TotGPSPingsCnt_perTripID'],df.loc[0,'depot_description'],df.loc[0,'district_description'],df.loc[0,'taluka_descr'],df.loc[0,'city_descr']]],columns= ['Key','Trip ID','Plant','City Code','Lead','TripID_rcrdCnt','depot_description','district_description','taluka_descr','city_descr'])
                TripID_WRouteDetails_Unq = df[['Orgn_lat','Orgn_lng','Dest_lat','Dest_lng','latitude','longitude','DtTimeSeq','Depot','depot_description','district_description','taluka_descr','city_descr']].drop_duplicates()
                TripID_WRouteDetails_Unq = TripID_WRouteDetails_Unq.drop_duplicates(subset= ['Orgn_lat','Orgn_lng','Dest_lat','Dest_lng','latitude','longitude','Depot','depot_description','district_description','taluka_descr','city_descr'],keep = 'first')
                TripID_WRouteDetails_Unq = TripID_WRouteDetails_Unq[TripID_WRouteDetails_Unq['latitude'] != 0]
                TripID_WRouteDetails_Unq.reset_index(inplace = True, drop = True)
                TripID_WRouteDetails_Unq['latitude'] = pd.to_numeric(TripID_WRouteDetails_Unq['latitude'])
                TripID_WRouteDetails_Unq['longitude'] = pd.to_numeric(TripID_WRouteDetails_Unq['longitude'])
                TripID_WRouteDetails_Unq = TripID_WRouteDetails_Unq.sort_values(['DtTimeSeq'], ascending=True).reset_index(drop=True)
                df2['HvrsineAggrtd'] = 0
                if  (len(TripID_WRouteDetails_Unq)/df2.loc[0,'TripID_rcrdCnt'] < .2) | (len(TripID_WRouteDetails_Unq) < 2) :
                    df2['TripID_EliminatedReason'] = 'Number of distinct GPS pings too low: % of total ping count < 20% ' 
                    df2['TripID_EliminationKPI'] = round(len(TripID_WRouteDetails_Unq)/df2.loc[0,'TripID_rcrdCnt']*100,2)
                    df2['Waypoint_Cnt'] = 0
                    MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)
                else :
                    
                    from_lat_lng = TripID_WRouteDetails_Unq[['Orgn_lat', 'Orgn_lng']].to_numpy()
                    # from_lat_lng=from_lat_lng.astype(np.float)
                    to_lat_lng = TripID_WRouteDetails_Unq[['latitude', 'longitude']].to_numpy()
                    # to_lat_lng=to_lat_lng.astype(np.float)
                    TripID_WRouteDetails_Unq.loc[:,'HvrsnDistOrgn_LatLng'] = pd.Series(haversine_vector(from_lat_lng, to_lat_lng, Unit.KILOMETERS))
                    #Cases where Destination Lat-Long is nan, then take destination lat-long corresponding to maximum distance of lat-long from origin
                    if len(TripID_WRouteDetails_Unq[TripID_WRouteDetails_Unq['Dest_lat'].isnull()])>0:
                        Dest_latLng = TripID_WRouteDetails_Unq[TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng'] == max(TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng'])]
                        Dest_latLng.reset_index(inplace=True,drop=True)
                        TripID_WRouteDetails_Unq['Dest_lat'] = Dest_latLng.loc[0,'latitude']
                        TripID_WRouteDetails_Unq['Dest_lng'] = Dest_latLng.loc[0,'longitude']
                    
                    from_lat_lng = TripID_WRouteDetails_Unq[['Dest_lat', 'Dest_lng']].to_numpy()
                    to_lat_lng = TripID_WRouteDetails_Unq[['latitude', 'longitude']].to_numpy()
                    
                    TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng'] = pd.Series(haversine_vector(from_lat_lng, to_lat_lng, Unit.KILOMETERS))
            
                    v1 = TripID_WRouteDetails_Unq[TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng'] == min(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng'])]['HvrsnDistOrgn_LatLng']
                    if (TripID_WRouteDetails_Unq.loc[0,'HvrsnDistOrgn_LatLng'] - TripID_WRouteDetails_Unq.loc[len(TripID_WRouteDetails_Unq)-1,'HvrsnDistOrgn_LatLng'] > max(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistOrgn_LatLng'])*.5) :
                        df2['TripID_EliminatedReason'] = 'Return trip GPS pings found: Distance between start lat-long and origin(Plant) lat-long:' 
                        df2['TripID_EliminationKPI'] = TripID_WRouteDetails_Unq.loc[0,'HvrsnDistOrgn_LatLng']
                        df2['Waypoint_Cnt'] = 0
                        MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)
                    
                    else:
                        
                        if (min(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng'])/max(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng']) > .1) :
                            df2['TripID_EliminatedReason'] = 'Destination GPS ping not found: min/max of destination to GPS pings distance >> 10% ' 
                            df2['TripID_EliminationKPI'] = round(min(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng'])/max(TripID_WRouteDetails_Unq.loc[:,'HvrsnDistDest_LatLng'])*100,2)
                            df2['Waypoint_Cnt'] = 0
                            MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)
                    #        TripID_WRouteDetails_Unq['OrgnXdest'] = TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng']*TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng']
                        else :
                            if (TripID_WRouteDetails_Unq.loc[0,'HvrsnDistOrgn_LatLng']/v1.iloc[0] > .1) :
                                df2['TripID_EliminatedReason'] = 'Trip GPS ping start midway: Distance between Start GPS pings vs Src lat-long from FcltyMaster  >> 10% ' 
                                df2['TripID_EliminationKPI'] = round((TripID_WRouteDetails_Unq.loc[0,'HvrsnDistOrgn_LatLng']/v1.iloc[0])*100,2)
                                df2['Waypoint_Cnt'] = 0
                                MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)
                        
                            else:
                                Orgn_LatLngDistPrcntl85 = min(TripID_WRouteDetails_Unq.loc[0,'HvrsnDistDest_LatLng'], np.percentile(TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng'], 85))
                                Dest_LatLngDistPrcntl5 = max(np.percentile(TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng'], 5),.01)
                                
                                #Mark row with > 85% orgn-latLng distance and <5% dest-ltLng distance
                                OnwardTrip_DestLtLngGPS = TripID_WRouteDetails_Unq[(TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng'] > Orgn_LatLngDistPrcntl85) & (TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng'] < Dest_LatLngDistPrcntl5)]
                                if len(OnwardTrip_DestLtLngGPS) == 0:
                                    #Orgn_LatLngDistPrcntl80 = min(TripID_WRouteDetails_Unq.loc[0,'HvrsnDistDest_LatLng']*.99, np.percentile(TripID_WRouteDetails_Unq['HvrsnDistOrgn_LatLng'], 75))
                                    Dest_LatLngDistPrcntl5 = np.percentile(TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng'], 5)
                                    #Mark row with > 85% orgn-latLng distance and <5% dest-ltLng distance
                                    OnwardTrip_DestLtLngGPS = TripID_WRouteDetails_Unq[(TripID_WRouteDetails_Unq['HvrsnDistDest_LatLng'] < Dest_LatLngDistPrcntl5)]
                                    
                                    
                                #Filter for rcords for onward journey only from the GPS data
                                OnwardJourneyGPSData =  TripID_WRouteDetails_Unq[TripID_WRouteDetails_Unq.index <= min(OnwardTrip_DestLtLngGPS.index)]
                                OnwardJourneyGPSData['latitude_4dec']=round(OnwardJourneyGPSData['latitude'],4)
                                OnwardJourneyGPSData['longitude_4dec']=round(OnwardJourneyGPSData['longitude'],4)
                                # gps_pings = OnwardJourneyGPSData[['latitude_4dec','longitude_4dec']].drop_duplicates().reset_index(drop=True)
                                
                                present_truck_gps_attributes=present_truck_gps_attributes_raw[['road','village','city','county','state_district','state','ISO3166-2-lvl4','postcode','latitude_4dec','longitude_4dec']].drop_duplicates().reset_index(drop=True)
                                OnwardJourneyGPSData_1=pd.merge(OnwardJourneyGPSData,present_truck_gps_attributes[['road','village','city','county','state_district','state','ISO3166-2-lvl4','postcode','latitude_4dec','longitude_4dec']],how='left',on=['latitude_4dec','longitude_4dec'])
                                
                                lat_long_req_hit = OnwardJourneyGPSData_1.loc[((OnwardJourneyGPSData_1['road'].isnull()) &(OnwardJourneyGPSData_1['village'].isnull()) & (OnwardJourneyGPSData_1['city'].isnull()) & (OnwardJourneyGPSData_1['state_district'].isnull()) &(OnwardJourneyGPSData_1['state'].isnull()) & (OnwardJourneyGPSData_1['ISO3166-2-lvl4'].isnull()) & (OnwardJourneyGPSData_1['postcode'].isnull()))]
                                specific_trip_exist=OnwardJourneyGPSData_1[(OnwardJourneyGPSData_1[['road','village','city','county','state_district','state','ISO3166-2-lvl4','postcode']].notnull()).any(1)]
                                specific_trip_exist=specific_trip_exist[['road','village','city','county','state_district','state','ISO3166-2-lvl4','postcode','latitude_4dec','longitude_4dec']].drop_duplicates().reset_index(drop=True)
                                gps_pings = lat_long_req_hit[['latitude_4dec','longitude_4dec']].drop_duplicates().reset_index(drop=True)
                                
                            
                                #Axestrack_MergeGPS = MonitoredTrip_Axestrack.copy()
                                
                                specific_trip = pd.merge(gps_pings,specific_trip_[['latitude_4dec','longitude_4dec','ISO3166-2-lvl4']],on = ('latitude_4dec','longitude_4dec'),how='left') #gps_pings.copy() #pd.concat([specific_trip,gps_pings],ignore_index=True) 
                                specific_trip = specific_trip[specific_trip['ISO3166-2-lvl4'].isnull()]
                                del specific_trip['ISO3166-2-lvl4']
                                specific_trip = specific_trip.drop_duplicates().reset_index(drop=True)
                                
                                if len(specific_trip) > 0:
                                    i=0
                                    for i in range(0, len(specific_trip)):
                                        #start = time.time()
                                        lat1 = specific_trip.loc[i, "latitude_4dec"]
                                        lon1 = specific_trip.loc[i, "longitude_4dec"]
                                    
                                    
                                    
                                        print(i,lat1,lon1)
                                        location1 = geolocator.reverse(str(lat1)+","+str(lon1),timeout=None).raw['address']
                                        print(location1)
                                    
                                        try:
                                            specific_trip.loc[i, "road"] = location1["road"]
                                        except:
                                            pass
                                    
                                        try:
                                            specific_trip.loc[i, "village"] = location1["village"]
                                        except:
                                            pass
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "city"] = location1["city"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "county"] = location1["county"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "state_district"] = location1["state_district"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "state"] = location1["state"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "ISO3166-2-lvl4"] = location1["ISO3166-2-lvl4"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        try:
                                            specific_trip.loc[i, "postcode"] = location1["postcode"]
                                        except:
                                            pass
                                    
                                    
                                    
                                        master_df = pd.concat([master_df, specific_trip], axis = 0, sort = False)
                                        #print(datetime.datetime.now())
                                
                                
                                #master_df = master_df[~master_df['ISO3166-2-lvl4'].isnull()]
                                #master_df = master_df[master_df['ISO3166-2-lvl4'].isnull()]
                                #specific_trip_ = specific_trip_[~specific_trip_['ISO3166-2-lvl4'].isnull()]
                                specific_trip_ = pd.concat([specific_trip_,specific_trip],ignore_index=True)
                                specific_trip_ = pd.concat([specific_trip_,specific_trip_exist],ignore_index=True)
                                # df['latitude'] = df['latitude'].astype(float)
                                # df['longitude'] = df['longitude'].astype(float)
                                # specific_trip_.info()
                                OnwardJourneyGPSData = pd.merge(OnwardJourneyGPSData,specific_trip_,how='inner',on=('latitude_4dec','longitude_4dec'))
                                present_truck_gps_attributes_raw = present_truck_gps_attributes_raw.append(OnwardJourneyGPSData[['latitude', 'longitude', 'road', 'village', 'city', 'county','state_district', 'state', 'ISO3166-2-lvl4', 'postcode','latitude_4dec', 'longitude_4dec']]).drop_duplicates().reset_index(drop=True)
                                
                                
                                # TripID_WRouteDetails_Unq=pd.merge(df,TripID_WRouteDetails_Unq,how='inner',on=('latitude','longitude'))
                                if len(OnwardJourneyGPSData[OnwardJourneyGPSData['ISO3166-2-lvl4'].isnull()]) == 0: print('Good to Go')

                                OnwardJourneyGPSData['Waypoint'] = OnwardJourneyGPSData['road'].fillna(OnwardJourneyGPSData['village'])
                                OnwardJourneyGPSData['Waypoint'] = OnwardJourneyGPSData['Waypoint'].fillna(OnwardJourneyGPSData['county'])
                                OnwardJourneyGPSData['Waypoint'] = OnwardJourneyGPSData['Waypoint'].fillna(OnwardJourneyGPSData['city'])
                                
                                #Break the round trip data to only onward journey
                                #Percentile calculation for Orgn-LatLng and Dest-LatLng
                                
                                #Split Waypoint for road
                                try:
                                    OnwardJourneyGPSData[['Waypoint', 'Waypoint2']] = OnwardJourneyGPSData['Waypoint'].str.split(';', 1, expand=True)
                                except:
                                    pass
                
                                
                                #Mark or flag rows with a distance gap of min 10km from sequenced lat-long
                                i = 0
                                OnwardJourneyGPSData['Dist10km_Flag'] = 0
                                OnwardJourneyGPSData['WaypointChng'] = 0
                                OnwardJourneyGPSData['NullValue'] = 0
                                OnwardJourneyGPSData['Waypoint'] = OnwardJourneyGPSData['Waypoint'].fillna('Null')
                                strtGPS = OnwardJourneyGPSData.loc[i,'HvrsnDistOrgn_LatLng']
                                strtWaypoint = OnwardJourneyGPSData.loc[i,'Waypoint']
                                
                                while (i < len(OnwardJourneyGPSData)):
                                    if OnwardJourneyGPSData.loc[i,'Waypoint'] == 'Null' : OnwardJourneyGPSData.loc[i,'NullValue'] = 1
                                    if OnwardJourneyGPSData.loc[i,'HvrsnDistOrgn_LatLng'] - strtGPS > 10 :
                                        OnwardJourneyGPSData.loc[i,'Dist10km_Flag'] = 1
                                        strtGPS = OnwardJourneyGPSData.loc[i,'HvrsnDistOrgn_LatLng']
                                    if  OnwardJourneyGPSData.loc[i,'Waypoint'] != strtWaypoint :
                                        OnwardJourneyGPSData.loc[i,'WaypointChng'] = 1
                                        strtWaypoint = OnwardJourneyGPSData.loc[i,'Waypoint']
                                    i = i+1
                                    
                                #Flag waypoints: extracting waypoints for google hits from the given GPS points of onward journey
                                i=0
                                StrtPoint_Lat = OnwardJourneyGPSData.loc[i,'latitude']
                                StrtPoint_Lng = OnwardJourneyGPSData.loc[i,'longitude']
                                Waypoint = OnwardJourneyGPSData.loc[i,'Waypoint']
                                Waypoint_LatLng = pd.DataFrame([[1,StrtPoint_Lat,StrtPoint_Lng, Waypoint]],columns = ['SNo','Lat','Long','Waypoint'])
                                i=1
                                k = 1
                                NullValueFlag = 0
                                Distncgrtr10Flag = 0
                                while i <len(OnwardJourneyGPSData):
                                    if OnwardJourneyGPSData.loc[i,'Dist10km_Flag'] == 1:
                                        Distncgrtr10Flag = 1
                                    if OnwardJourneyGPSData.loc[i,'WaypointChng'] == 1 & Distncgrtr10Flag == 1 :
                                        k = k+1
                                        Waypoint_LatLng = pd.concat([Waypoint_LatLng,pd.DataFrame([[k,OnwardJourneyGPSData.loc[i,'latitude'],OnwardJourneyGPSData.loc[i,'longitude'],OnwardJourneyGPSData.loc[i,'Waypoint']]],columns = ['SNo','Lat','Long','Waypoint'])],ignore_index=False)
                                        Distncgrtr10Flag = 0
                                    i = i + 1    
                                    
                                #Appending dest lat-long    
                                Waypoint_LatLng = pd.concat([Waypoint_LatLng,pd.DataFrame([[k+1,OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'latitude'],OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'longitude'],OnwardJourneyGPSData.loc[len(OnwardJourneyGPSData)-1,'Waypoint']]],columns = ['SNo','Lat','Long','Waypoint'])],ignore_index=False)
                                #Lag lat-long values to find distance between two consecutive gps points
                                Waypoint_LatLng['Key'] = 1
                                Waypoint_LatLng['lag_lat'] = Waypoint_LatLng.groupby(['Key'])['Lat'].shift(1)
                                Waypoint_LatLng['lag_lng'] = Waypoint_LatLng.groupby(['Key'])['Long'].shift(1)
                                Waypoint_LatLng['lag_lat'] = Waypoint_LatLng['lag_lat'].fillna(Waypoint_LatLng['Lat'])
                                Waypoint_LatLng['lag_lng'] = Waypoint_LatLng['lag_lng'].fillna(Waypoint_LatLng['Long'])
                                
                                Waypoint_LatLng.reset_index(inplace=True, drop= True)
                                
                                
                                from_lat_lng = Waypoint_LatLng[['lag_lat', 'lag_lng']].to_numpy()
                                to_lat_lng = Waypoint_LatLng[['Lat', 'Long']].to_numpy()
                                
                                Waypoint_LatLng.loc[:,'HvrsnDist_WayPointLatLng'] = pd.Series(haversine_vector(from_lat_lng, to_lat_lng, Unit.KILOMETERS))
                                #Aggregating hvrsine distance of each trip ID to filter out trip IDs with insufficient data
                                df2['HvrsineAggrtd'] = sum(Waypoint_LatLng['HvrsnDist_WayPointLatLng'])
                                Waypoint_LatLng = Waypoint_LatLng[(Waypoint_LatLng['HvrsnDist_WayPointLatLng'] > 10) | (Waypoint_LatLng['HvrsnDist_WayPointLatLng'] == 0)]
                                df2['TripID_EliminatedReason'] = '-' 
                                df2['TripID_EliminationKPI'] = 0
                                df2['Waypoint_Cnt'] = len(Waypoint_LatLng)
                                Waypoint_LatLng['TripID'] = df2.loc[0,'Trip ID']
                                Waypoint_LatLng['Plant'] = df2.loc[0,'Plant']
                                Waypoint_LatLng['City Code'] = df2.loc[0,'City Code']
                                Waypoint_LatLng['depot_description'] = df2.loc[0,'depot_description']
                                Waypoint_LatLng['district_description'] = df2.loc[0,'district_description']
                                Waypoint_LatLng['taluka_descr'] = df2.loc[0,'taluka_descr']
                                Waypoint_LatLng['city_descr'] = df2.loc[0,'city_descr']
                                TripID_Waypoint_LatLng = pd.concat([TripID_Waypoint_LatLng,Waypoint_LatLng],ignore_index=True)
                                TripID_Waypoint_LatLng.reset_index(inplace=True,drop=True)
                                MD_Trip = pd.concat([MD_Trip,df2],ignore_index = True)
                        #       'depot_description','district_description','taluka_descr','city_descr'

                
                TIDCnt = TIDCnt + 1    
                    #Filter out distinct waypoint combinations
                    #AxestrackMarToAug_MergeGPS.to_csv("D:\\ABG\\FARE\\Data\\SED\\MonitoredTrips\\Axestrack_sep2022.csv",index=False)
                    #TripID_sample.to_csv("D:\\ABG\\FARE\\Data\\SED\\MonitoredTrips\\TripID_Fltrd.csv",index=False)
                    #GPS_TripID_MartoAug.to_csv("D:\\ABG\\FARE\\Data\\SED\\MonitoredTrips\\TripID_RcnrdCnt_GPSUTCL_Data_Sep.csv",index=False)
            
            #WWaypoint_MD_Trip = pd.DataFrame() #MD_Trip[MD_Trip['Waypoint_Cnt'] > 0]
            MD_Trip_ = pd.concat([MD_Trip_,MD_Trip],ignore_index=True)
            # MD_Trip_.to_csv('MD_trip_Dec_22.csv')
            MD_Trip = MD_Trip[MD_Trip['Waypoint_Cnt'] > 0]
            MD_Trip.reset_index(inplace=True,drop=True)
            WWaypoint_MD_Trip = pd.concat([WWaypoint_MD_Trip,MD_Trip],ignore_index=True)
            #Waypoint_LatLng_All = pd.DataFrame()
            Waypoint_LatLng_All = pd.concat([Waypoint_LatLng_All,TripID_Waypoint_LatLng],ignore_index=True)
            
            #selecting the best route for a plant-city code combination out of the recorded trip IDs
            MD_Trip['Lead_HvrsineMAPE'] = abs(MD_Trip['Lead'] - MD_Trip['HvrsineAggrtd'])/MD_Trip['Lead']
            UnqPlntCityCd_WypntMD = MD_Trip[['Plant','City Code']].drop_duplicates()
            UnqPlntCityCd_WypntMD.reset_index(inplace=True,drop=True)
            WWaypoint_MD_Trip_WminMAPE = pd.DataFrame()
            i2=0
            while (i2 < len(UnqPlntCityCd_WypntMD)):
                df = MD_Trip[(MD_Trip['Plant'] == UnqPlntCityCd_WypntMD.loc[i2,'Plant']) & (MD_Trip['City Code'] == UnqPlntCityCd_WypntMD.loc[i2,'City Code'])]
                if len(df) > 1:
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
                    
                    if len(df) > 1:
                        df = df[df['Waypoint_Cnt'] == max(df['WaypointScore'])]
                    
                WWaypoint_MD_Trip_WminMAPE = pd.concat([WWaypoint_MD_Trip_WminMAPE,df],ignore_index=True)
                i2 = i2 + 1
            
            WWaypoint_MD_Trip_WminMAPE.reset_index(inplace=True, drop = True)
            
            WWaypoint_MD_Trip_WminMAPE_ = pd.concat([WWaypoint_MD_Trip_WminMAPE_,WWaypoint_MD_Trip_WminMAPE],ignore_index=True)
            
            TripID_Waypoint_LatLng.rename(columns={'TripID':'Trip ID'},inplace=True)
            if len(WWaypoint_MD_Trip_WminMAPE) > 0:
                Waypoint_LatLng_All_FnlRouteSlctd = pd.merge(TripID_Waypoint_LatLng,WWaypoint_MD_Trip_WminMAPE[['Trip ID','Waypoint_Cnt']],on='Trip ID',how='inner')
                Waypoint_LatLng_All_FnlRouteSlctd_All = pd.concat([Waypoint_LatLng_All_FnlRouteSlctd_All,Waypoint_LatLng_All_FnlRouteSlctd],ignore_index=True)
        MD_Trip_.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\MD_trip_jan.csv",index=False)
        WWaypoint_MD_Trip_WminMAPE_.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\waypoint_summary_jan.csv",index=False)
        Waypoint_LatLng_All_FnlRouteSlctd_All.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\waypoint_trip_id_level_jan.csv",index=False)
        present_truck_gps_attributes_raw.to_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\output datsets\gps_attributes_jan.csv",index=False)
    
        cntr1 = cntr1 + 1
    return MD_Trip_,WWaypoint_MD_Trip_WminMAPE_,Waypoint_LatLng_All_FnlRouteSlctd_All,present_truck_gps_attributes_raw
    






if __name__ == '__main__':

    MonthRun = "jan23"
    MonitoredTripGPSData_Input_mypath = "C:\\Users\\nikhil.upadhyay-v\\Downloads\\SED\\" + MonthRun + "\\"

    
    
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
    # Latitude = "25.594095"
    # Longitude = "85.137566"
    
    cur = database_connection(database_name_prod,host_name_prod,port_prod,user_name_prod,user_password_prod)

    facility_master,node_facility_master,customer_master,node_customer_master,node_address=fetching_data(cur)[0:5]
    
    Waypoint_WCustFclty_WLatLng=fetching_lat_long_cust_facility(facility_master,node_facility_master,customer_master,node_customer_master,node_address)
    
    dataj =importing_monitored_trip_data(MonthRun,MonitoredTripGPSData_Input_mypath)
    # dataj.to_csv('/home/viplav/JAN_Output/dataj.csv')
    unique_trip_id_in_GPS_data=Unique_list_tripid_GPS()

    unique_trip_id_Monitored = dataj.loc[dataj['Lead_SAP'] > 40]
    unique_trip_id_Monitored=unique_trip_id_Monitored[['Trip ID']].drop_duplicates().reset_index(drop=True)
    MD_Trip_,WWaypoint_MD_Trip_WminMAPE_,Waypoint_LatLng_All_FnlRouteSlctd_All,present_truck_gps_attributes_raw=way_point_analysis(dataj,unique_trip_id_Monitored,unique_trip_id_in_GPS_data,Waypoint_WCustFclty_WLatLng)[0:4]
   

    # MD_Trip_.to_csv('/home/viplav/JAN_Output/MD_trip_jan.csv',index=False)
    # WWaypoint_MD_Trip_WminMAPE_.to_csv('/home/viplav/JAN_Output/waypoint_summary_jan.csv',index=False)
    # Waypoint_LatLng_All_FnlRouteSlctd_All.to_csv('/home/viplav/JAN_Output/waypoint_trip_id_level_jan.csv',index=False)
    # present_truck_gps_attributes_raw.to_csv('/home/viplav/JAN_Output/gps_attributes_jan.csv',index=False)


