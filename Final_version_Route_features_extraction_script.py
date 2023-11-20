# -*- coding: utf-8 -*-
"""
Created on Mon May  8 13:33:32 2023

@author: viplav.singla-v
"""

import pandas as pd
import numpy as np
import googlemaps  
import statistics
import scipy.stats

gmaps = googlemaps.Client(key = "AIzaSyATf6n8cUm7sck9ofp9cfroioX5LFrrHpE")

Waypoint_LatLng_All_FnlRouteSlctd_All = pd.read_excel('sample_route.xlsx')
testList =  Waypoint_LatLng_All_FnlRouteSlctd_All.copy() #pd.read_csv("D:\\ABG\\FARE\\Data\\SED\\RouteWaypoint_50PlntCity.csv")

data_OrgDest = testList[testList['SNo'] == 1]
data_OrgDest.rename(columns={'lag_lat':'Src_Lat','lag_lng':'Src_Long'},inplace=True)
data_OrgDest.reset_index(inplace=True,drop=True)
data_OrgDest_ = testList[testList['SNo'] >= testList['Waypoint_Cnt']] 
data_OrgDest_ = data_OrgDest_.drop_duplicates(subset=['Plant','City Code'],keep='last') 
data_OrgDest_.rename(columns={'Lat':'Dest_Lat','Long':'Dest_Long'},inplace=True)
data_OrgDest_.reset_index(inplace=True,drop=True)
data_OrgDest = pd.merge(data_OrgDest[['Plant','City Code','Src_Lat','Src_Long']], data_OrgDest_[['Plant','City Code','Dest_Lat','Dest_Long']],how='inner',on=('Plant','City Code'))

testList_ = testList[['Plant','City Code','lag_lat','lag_lng','Lat','Long','Waypoint']]
testList_.rename(columns={'Plant':'DELVRY_PLANT','City Code':'CITY_CODE','lag_lat':'Source_Lat','lag_lng':'Source_Long','Lat':'lat'},inplace=True)

#################################################################################################################
feature_ext=testList_[["DELVRY_PLANT","CITY_CODE","Source_Lat","Source_Long","lat","Long",'Waypoint']] #"Google_Dist","geocode_time"]]
feature_ext.drop_duplicates(inplace=True)
feature_ext.rename(columns={"lat":"Latitude","Long":"Longitude"},inplace=True)

###############################################################################################


'''
feature extraction 

'''

NH_c = ["NH", "Hwy", "Highway", "HIGHWAY", "Ring", "ring", "RING", "HWY", "hwy", "highway", "AH", "Asian Highway"]
SH_c = ["SH"]

 
#data=feature_ext.copy()

def feature_extraction(data,OrgDest):
    data = feature_ext.copy()
    
    OrgDest = data_OrgDest.copy()
    cols = ['mean_ele', 'median_ele', 'max_ele', 'min_ele','range_ele', 'std_ele', 'kurtosis_ele', 'skewness_ele','NH_Per', 'SH_Per', 'oth_Per', 'Plain_Per', 'Hilly_Per','Up_Hill_Per','Down_Hill_per','NH_Dist','SH_Dist','oth_Dist']
    temp = pd.DataFrame(columns=cols,index=range(len(OrgDest)))
    
    #data.reset_index(drop=True,inplace=True)
    i2=0
    df2 = pd.DataFrame()
    while i2 < len(OrgDest):
        
        data2 = data[(data['DELVRY_PLANT'] == OrgDest.loc[i2,'Plant']) & (data['CITY_CODE'] == OrgDest.loc[i2,'City Code'])]
        data2.reset_index(inplace=True,drop=True)
        df = pd.DataFrame()
        step_len2 = 0
        temp2 = pd.DataFrame(columns=cols,index=range(len(data2)))
        # i=0
        for i in range(len(data2)):
            # i=1
            print(i)
            source = (data2['Source_Lat'][i],data2['Source_Long'][i])
            destination = (data2['Latitude'][i],data2['Longitude'][i])
            
            try:
                print('maps_hits')
                # Get the Direction
                check1 = gmaps.directions(source,destination)
    
                #result = gmaps.distance_matrix(source, destination, mode='driving')
    
                step_len = len(check1[0]['legs'][0]['steps'])
    
                step_len2 = step_len2 + step_len
    
                
                i3 = 0
                for j in range(step_len2 - step_len, step_len2):
                    # print("step_len2:",j)
                    df.loc[j,'lat'] = check1[0]['legs'][0]['steps'][i3]['end_location']['lat']
                    df.loc[j,'lng'] = check1[0]['legs'][0]['steps'][i3]['end_location']['lng']
                    df.loc[j,'distance'] = check1[0]['legs'][0]['steps'][i3]['distance']['value']
                    df.loc[j,'comment'] = check1[0]['legs'][0]['steps'][i3]['html_instructions']
                    i3 = i3+1
                    
                # Extract Elevation data
                print('elevation_hit')
                elevation_data = gmaps.elevation_along_path([[float(str(data2['Source_Lat'][i])),float(str(data2['Source_Long'][i]))],[float(str(data2['Latitude'][i])),float(str(data2['Longitude'][i]))]],samples=max(step_len,2))
                if i < 2: elevation_data2 = elevation_data
                else : elevation_data2 =   elevation_data2 + elevation_data
                
            except:
                    temp2.loc[i,:] = np.nan
                    
        for k in range(len(df)):
                if any(ext in df['comment'][k] for ext in NH_c):
                    df.loc[k,'NH_flag'] = 1
                else:
                    df.loc[k,'NH_flag'] = 0
                if any(ext in df['comment'][k] for ext in SH_c) & (df.loc[k,'NH_flag'] == 0):
                    df.loc[k,'SH_flag'] = 1
                else:
                    df.loc[k,'SH_flag'] = 0
     
                
    
        total_dist = df['distance'].sum()
        non_NH_dist = df.groupby(['NH_flag'])['distance'].sum()[0.0]
        non_SH_dist = df.groupby(['SH_flag'])['distance'].sum()[0.0]
 
  
 
        NH_dist = total_dist - non_NH_dist
        SH_dist = total_dist - non_SH_dist
        HighWy_dist = NH_dist + SH_dist
        oth_dist = total_dist - NH_dist - SH_dist
    
     
                # Elevation feature extraction
    
     
        elev_list=[]
        elev_list1 = []
        elev_list2 = [0]
        
        for l in range(len(elevation_data2)):
                elev_list1.append(elevation_data2[l]['elevation'])
                elev_list2.append(elevation_data2[l]['elevation'])
        elev_list2=elev_list2[:-1]
        elev_list=list(np.array(elev_list1) - np.array(elev_list2))
        elev_list[0]=0
        
        delta_distances = []
        slopes = []
        i=1
        for i in range(1,len(elevation_data2)):
                delta_distance = (np.sqrt((elevation_data2[i]['location']['lat'] - elevation_data2[i-1]['location']['lat'])**2 + (elevation_data2[i]['location']['lng'] - elevation_data2[i-1]['location']['lng'])**2))*111.3*1000
                slope = (elev_list[i])/delta_distance
                slopes.append(slope)
                delta_distances.append(delta_distance)
        Up_hill = 0   
        down_hill = 0
        hilly = 0
        for i in range(len(slopes)):
            
            if slopes[i]>=0.01:
                Up_hill=delta_distances[i]+Up_hill
            if slopes[i]<= -0.01:
                down_hill=delta_distances[i]+down_hill
            if abs(slopes[i])>=0.01:
                hilly=delta_distances[i]+hilly
                
                
        Hilly_Per =hilly/sum(delta_distances) *100 
        Plain_Per = (100-Hilly_Per)
        Up_Hill_Per = Up_hill/sum(delta_distances) *100
        Down_Hill_per = down_hill/sum(delta_distances) *100
                
            
        # slopes=slopes.loc[slopes[]]    
        NH_Per = NH_dist/total_dist
        SH_Per = SH_dist/total_dist
        HighWy_Per = HighWy_dist/total_dist
        oth_Per = oth_dist/total_dist
     
        mean_ele = statistics.mean(elev_list)
        median_ele = statistics.median(elev_list)
        std_ele = statistics.stdev(elev_list)
        kurtosis_ele = scipy.stats.kurtosis(elev_list)
        skewness_ele = scipy.stats.skew(elev_list)
        min_ele = min(elev_list)
        max_ele = max(elev_list)
        range_ele = max_ele-min_ele
        # Plain_Per,Hilly_Per = Plain_Hilly_Per(elev_list)


        temp.loc[i2,"NH_Dist"] = NH_dist
        temp.loc[i2,"SH_Dist"] = SH_dist
        temp.loc[i2,"oth_Dist"] = oth_dist
        
        temp.loc[i2,"NH_Per"] = NH_Per*100
        temp.loc[i2,"SH_Per"] = SH_Per*100
        temp.loc[i2,"oth_Per"] = oth_Per*100
        temp.loc[i2,"mean_ele"] = mean_ele
        temp.loc[i2,"median_ele"] = median_ele
        temp.loc[i2,"std_ele"] = std_ele
        temp.loc[i2,"kurtosis_ele"] = kurtosis_ele
        temp.loc[i2,"skewness_ele"] = skewness_ele
        temp.loc[i2,"min_ele"] = min_ele
        temp.loc[i2,"max_ele"] = max_ele
        temp.loc[i2,"range_ele"] = range_ele
        temp.loc[i2,"Plain_Per"] = Plain_Per
        temp.loc[i2,"Hilly_Per"] = Hilly_Per
        temp.loc[i2,"Up_Hill_Per"] = Up_Hill_Per
        temp.loc[i2,"Down_Hill_per"] = Down_Hill_per
        i2 = i2 + 1
   # except:
    #    temp.loc[i,:] = np.nan
        
             
    df2 = pd.concat([OrgDest,temp],axis=1,ignore_index=True)
    
    df2.columns = list(OrgDest.columns)+cols
    return df2


route_features = feature_extraction(feature_ext,data_OrgDest)
# route_features = pd.to_csv('final_results.csv')
