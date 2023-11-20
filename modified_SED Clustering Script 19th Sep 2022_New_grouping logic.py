import pandas as pd
import numpy as np
# from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from openpyxl.utils import get_column_letter
from sklearn.neighbors import NearestNeighbors
import psycopg2 as pg
import pandas.io.sql as psql
from kneed import KneeLocator
from sklearn.cluster import DBSCAN


clusteringSlab = 10 #input

# df=Plant_City_Truck_3Month_dispatch.copy()
df = pd.read_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\Plant_City_Truck_3Month_dispatch.csv")
df = df.drop('Unnamed: 0', axis= 1)

Unit_master =pd.read_excel(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\Unit Master.xlsx")
# Unit_master['Plant Code']=Unit_master['Plant Code'].astype(str)
Unit_master =Unit_master[['Plant Code','Unit Code']].drop_duplicates()

df =pd.merge(df,Unit_master,left_on='delvry_plant',right_on='Plant Code',how='left')

# =====Data Cleaning step that needs to sit on top of all calculations (Boxplot and Scatterplot)=========
df['lead'] = df['lead'].fillna(0).astype(float)
df = df[(df.lead > 0) & (df.lead < 2000)]
df['integration_flag'] = df['integration_flag'].fillna(0)
df['cluster_flag'] = df['cluster_flag'].fillna(0)
df['union_flag'] = df['union_flag'].fillna(0)

df_quantity = df.copy()
df_quantity=df_quantity.fillna(0)

def Scatter(df_quantity):
    
    slabDiff = clusteringSlab
    
    # All route characteristics
    # allFeatures = ['nh_per', 'sh_per', 'plain_per', 'hilly_per','total_restriction_time', 'integration_flag', 'cluster_flag', 'union_flag']
    
    #If no features are selected by user, all features to be considered
    # clustPass = allFeatures if len(clust) == 0 else clust
    
    clustPass=['nh_per', 'sh_per', 'plain_per', 'hilly_per','total_restriction_time', 'integration_flag', 'cluster_flag', 'union_flag']
    
    inputFlags = [x for x in ['integration_flag', 'cluster_flag', 'union_flag'] if x in clustPass]
    inputFeatures = [x for x in clustPass if x not in inputFlags]
            
    # if ('plain_per' in inputFeatures):
    #     inputFeatures.remove('plain_per')
    #     if('hilly_per' not in inputFeatures): inputFeatures.append('hilly_per')

    # Removing null values in the selected features as it may hinder the clustering algorithm
    df_clean=df_quantity.dropna(subset=inputFeatures)
    df_clean.drop_duplicates(inplace=True)    
       
    # Dropping duplicates. In case multiple route characteristics are obtained for single route, first one is taken
    df_features=df_clean[['delvry_plant','city_code','nh_per','sh_per','hilly_per','plain_per','total_restriction_time']]
    df_features=df_features.drop_duplicates(subset=['delvry_plant','city_code'],keep='first')
    
    
    if True:       # Till Route features are corrected (majority zeroes are corrected), this rule to be used
        df_features_z = df_features.copy()
        for j in inputFeatures:       # Calculating Z Score of every feature, excluding the zeroes
            
            # j = 'nh_per'
            dfIter = df_features[['delvry_plant', 'city_code', j]].reset_index(drop = True)
                    
            dfIterZero = dfIter[dfIter[j] == 0].reset_index(drop = True) # Separating the zeroes and Non Zeroes
            dfIterNonZero = dfIter[~(dfIter[j] == 0)].reset_index(drop = True)
            
            dfIterZero[j+'_z'] = '-'       # Assigning z-score of zeroes as -
            dfIterZero[j + '_Alphabecket'] = 'A'       # Bucket assignment of zeroes 
            dfIterZero = dfIterZero.drop([j + '_z'], axis = 1)
                                
            if dfIterNonZero[j].nunique()==1:       # in case there is same value in features columns, then exempt it from z score calculation(as zscore for it is NaN)
                dfIterNonZero[j+'_z'] = '-'
                dfIterNonZero[j + '_Alphabecket'] = 'B' 
                dfIterNonZero = dfIterNonZero.drop([j + '_z'], axis = 1)
                
            else:             
                dfIterNonZero[j+'_z'] = (dfIterNonZero[j] - dfIterNonZero[j].mean())/dfIterNonZero[j].std()                        
                dfIterNonZero[j + '_bkt'] = pd.cut(dfIterNonZero[j + '_z'], 
                                                    bins = [min(dfIterNonZero[j + '_z'])-1] + [-1,0,1] + [max(dfIterNonZero[j + '_z'])+1]) if not dfIterNonZero.empty else np.nan       # Binning of Non Zeroes
                          
                dfIterNonZero[j + '_Alphabecket'] = '-'       # Bucket initiation of Non Zeroes
                for i in range(len(dfIterNonZero[j + '_bkt'].unique())):       # Bucket of every bin
                    dfIterNonZero.loc[dfIterNonZero[j + '_bkt'] == dfIterNonZero[j + '_bkt'].unique()[i],j + '_Alphabecket'] = chr(i + 67)       # Group starting from ASCII 67 as ASCII 65 is A and 66 is B (assigned to zeroes)

                dfIterNonZero = dfIterNonZero.drop([j + '_z', j + '_bkt'], axis = 1)
            
                       
            dfIter = dfIterZero.append(dfIterNonZero).drop(columns = j)       # Binding zeroes and non zeroes
            
            df_features_z = df_features_z.merge(dfIter, how = 'left')

        df_features_z['GroupComb'] = df_features_z[[lt + '_Alphabecket' for lt in inputFeatures]].apply(''.join, axis=1)       # Joining buckets of different features   
        df_features_z = df_features_z.drop([lt + '_Alphabecket' for lt in inputFeatures], 1)       # Dropping the individual groups
        groupAssignment = df_features_z.groupby('GroupComb', as_index = False).agg(count = ('delvry_plant', 'count'))
        groupAssignment.loc[groupAssignment['count'] < 3 ,'count'] = -1       # Groups with less than 3 samples are termed as MISC(ellaneous)
        
        groupAssignment = pd.concat([groupAssignment[groupAssignment['count'] != -1].reset_index(drop = True).reset_index().drop('count', 1).rename(columns = {'index':'Group'}),
                   groupAssignment[groupAssignment['count'] == -1].rename(columns = {'count':'Group'}).reset_index(drop = True)], 
                   axis = 0)       # Binding Zeroes (MISC) and the Non Zeroes after making there groups

        df_features_z = df_features_z.merge(groupAssignment, how = 'left').drop(['GroupComb'], 1)        
        df_features = df_features_z.copy()
        # inputFeatures = inputFeatures + ['plain_per'] if 'plain_per' in clust else inputFeatures
        inputFeatures = inputFeatures + ['plain_per'] 
        

    elif False:        
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(df_features[inputFeatures].values) #Passing only the selected features through the algorithm to be used for clustering
        
        # Finding optimum value for epsilon
        distMat = NearestNeighbors(n_neighbors = len(inputFeatures), algorithm='ball_tree').fit(scaled_data).kneighbors(scaled_data)[0]
    
        meanDists = pd.DataFrame({'Inds': list(range(scaled_data.shape[0])), 
                                  'Val': sorted(np.mean(distMat, axis = 1))})
    
        epsilon = meanDists.Val[KneeLocator(meanDists['Inds'], meanDists['Val'], S = 20, curve='convex', direction='increasing').knee] #Taking index of the value where the curve breaks.
        
        model = DBSCAN(eps = epsilon, min_samples=len(inputFeatures)) #Needs to be changed later
        model.fit(scaled_data)
        pred = model.fit_predict(scaled_data)
        df_features['Group'] = pred
    
    # Removing features from original table as multiple characteristics in same route were observed. Adding single features only to same routes
    df_clean_trimmed = df_clean.drop(columns=['nh_per','sh_per','hilly_per','plain_per','total_restriction_time'])
    df_clean_trimmed.drop_duplicates(inplace=True)
    
    df_clustered = pd.merge(df_clean_trimmed,df_features,on=['delvry_plant','city_code'],how='left')
    df_clustered.drop_duplicates(inplace=True)
    
    # Creating Clustering Slab
    df_clustered['clustering_slab'] = (((df_clustered['lead']//slabDiff) * slabDiff).astype(int).astype(str) + '-' + (((df_clustered['lead'] + slabDiff)//slabDiff) * slabDiff).astype(int).astype(str))    
        
    # Taking the ceil value for 'quantity' (Vehicle size)
    df_clustered['quantity']=np.ceil(df_clustered['quantity'])
    
    # Rounding off ptpk to 2 decimal places 
    df_clustered['ptpk']=round(df_clustered['ptpk'],2)  
    
    df_clustered['subGroup'] = (df_clustered['integration_flag'])*(2**2) + (df_clustered['cluster_flag'])*(2**1) + (df_clustered['union_flag'])*(2**0)
        
    # Imputing all non selected features as -
    df_clustered[np.setdiff1d(['nh_per','sh_per','hilly_per','plain_per','total_restriction_time'], inputFeatures)] = '-'
       
    # Marking Group names from integral values to Alphabets   
    df_clustered['Group'] = np.where(df_clustered['Group'] == -1, 18277, df_clustered['Group'])
    df_clustered['Group'] = df_clustered['Group'].apply(lambda x: get_column_letter(x+1))
    df_clustered['Group'] = np.where(df_clustered['Group'] == 'ZZZ', 'MISC', df_clustered['Group'])
    
    df_clustered['Group_report'] = df_clustered['Group'] + df_clustered['subGroup'].astype(int).astype(str)
    
    sample = df_clustered[['delvry_plant','city_code','direct_sto','clustering_slab','Group_report','truck_type','ptpk','integration_flag', 'cluster_flag', 'union_flag']].drop_duplicates().reset_index(drop=True)
    sample['Median_PTPK']=round(sample.groupby(['clustering_slab','Group_report','truck_type'] + inputFlags)['ptpk'].transform('median'), 2)
    del sample['ptpk']
    df_clustered =pd.merge(df_clustered,sample,on=['delvry_plant','city_code','direct_sto','clustering_slab','Group_report','truck_type','integration_flag', 'cluster_flag', 'union_flag'],how='left')
    
    
    df_clustered['Optimized'] = np.where(df_clustered['ptpk'] <= df_clustered['Median_PTPK'], 'Already Optimized', 'Need Optimization')

    df_clustered.drop(columns = 'subGroup', inplace = True)
    df_clustered.loc[df_clustered['Group'] == 'MISC', 'Group_report'] = 'MISC'    
    
    # Adding Pred BF column
    df_clustered['Predicted_BaseFreight']=df_clustered['Median_PTPK']*df_clustered['lead']
    
    # Adding monthly impact column
    df_clustered['Monthly_Impact']=np.where(((df_clustered['Median_PTPK']<df_clustered['ptpk'])&(df_clustered['Group_report']!='MISC')),(df_clustered['ptpk']-df_clustered['Median_PTPK'])*df_clustered['lead']*df_clustered['total_quantity'],0)/3
    
    # Adding monthly quantity column
    df_clustered['Monthly_quantity']=df_clustered['total_quantity']/3   
    
    df_clustered=df_clustered[['delvry_plant', 'city_code', 'truck_type', 'quantity', 'total_quantity',
            'lead', 'ptpk', 'direct_sto', 'type', 'zone', 'integration_flag',
            'cluster_flag', 'union_flag', 'base_freight', 'plant_type',
            'district_code', 'district_desc', 'depot_code', 'depot_desc',
            'region_code', 'region_desc', 'i2_taluka', 'i2_taluka_desc',
            'city_desc', 'plant_name', 'nh_per', 'sh_per', 'hilly_per',
            'plain_per', 'total_restriction_time', 'Group', 'clustering_slab',
            'Median_PTPK', 'Optimized', 'Group_report', 'Predicted_BaseFreight',
            'Monthly_Impact', 'Monthly_quantity','Remarks(Taluka Analysis)',
            'MonthlyAvgImpact(Taluka Analysis)','Unit Code']]
    
    output_1=df_clustered[['Group_report','clustering_slab','truck_type','Unit Code']].drop_duplicates()
    output_1 = output_1.groupby(['Group_report','clustering_slab','truck_type'])['Unit Code'].count().reset_index()
    output_1['Source Unit single multiple'] = np.where(output_1['Unit Code']>1,'Multiple','Single')
    del output_1['Unit Code']
    final_output = pd.merge(df_clustered,output_1,on=['Group_report','clustering_slab','truck_type'],how='left')
    # if len(output[['delvry_plant','city_code','truck_type','quantity','direct_sto']].drop_duplicates()) == len(output):print('Good to Go')
    
    final_output=final_output[['zone','Unit Code','delvry_plant','plant_name','city_code','city_desc','district_code', 'district_desc', 'depot_code', 'depot_desc',
                               'region_code', 'region_desc', 'i2_taluka', 'i2_taluka_desc','truck_type', 'quantity', 'total_quantity','lead', 'ptpk', 'direct_sto', 'type','integration_flag',
                               'cluster_flag', 'union_flag', 'base_freight', 'plant_type','nh_per', 'sh_per', 'hilly_per',
                               'plain_per', 'total_restriction_time', 'Group', 'clustering_slab',
                               'Median_PTPK', 'Optimized', 'Group_report', 'Predicted_BaseFreight',
                               'Monthly_Impact', 'Monthly_quantity','Remarks(Taluka Analysis)',
                               'MonthlyAvgImpact(Taluka Analysis)','Source Unit single multiple']]
    # final_output.to_csv('final_output_v.csv')
    return df_clustered



