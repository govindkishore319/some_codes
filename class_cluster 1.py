# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 15:17:57 2023

@author: govin
"""

import pandas as pd
import numpy as np
from string import ascii_uppercase
from sklearn.preprocessing import StandardScaler
from openpyxl.utils import get_column_letter
from sklearn.neighbors import NearestNeighbors
import psycopg2 as pg
import pandas.io.sql as psql
from kneed import KneeLocator
from sklearn.cluster import DBSCAN

class Clustering_Routes:
    def __init__(self, clustering_slab, df, unit):
        self.clustering_slab = clustering_slab
        self.df = df
        self.unit = unit

           
    def merge_data(self):
        df_unit_merged = pd.merge(self.df, self.unit, left_on= 'delvry_plant', right_on= 'Plant Code', how= 'left')
        return df_unit_merged
    
    def clean_data(self, df_unit_merged):
        df_unit_merged = df_unit_merged.fillna(0)
        df_unit_merged_cleaned = df_unit_merged[(df_unit_merged['lead'] > 0) & (df_unit_merged['lead'] < 2000)]
        return df_unit_merged_cleaned
    
    def calculate_z_scores(self, df_unit_merged_cleaned, input_features):
        df_clean = df_unit_merged_cleaned.drop_duplicates()
        df_features = df_clean[['delvry_plant', 'city_code'] + input_features]
        df_features = df_features.drop_duplicates(subset= ['delvry_plant', 'city_code'], keep= 'first')
        df_features_z = df_features.copy()
        
        for feature in input_features:
            df_iter = df_features[['delvry_plant', 'city_code', feature]].reset_index(drop= True)
            df_iter_zero = df_iter[df_iter[feature] == 0].reset_index(drop= True)
            df_iter_non_zero = df_iter[df_iter[feature] != 0].reset_index(drop= True)
            
            df_iter_zero[feature + '_z'] = '-'
            df_iter_zero[feature+ '_Alphabecket'] = 'A'
            df_iter_zero = df_iter_zero.drop([feature + '_z'], axis= 1)
            
            if df_iter_non_zero[feature].nunique() == 1:
                df_iter_non_zero[feature + '_z'] = '-'
                df_iter_non_zero[feature + '_Alphabecket'] = 'B'
                df_iter_non_zero = df_iter_non_zero.drop([feature + '_z'], axis=1)
            else:
                df_iter_non_zero[feature + '_z'] = (df_iter_non_zero[feature]- df_iter_non_zero[feature].mean())/df_iter_non_zero[feature].std()
                df_iter_non_zero[feature + '_bkt'] = pd.cut(df_iter_non_zero[feature + '_z'],
                                                           bins=[min(df_iter_non_zero[feature + '_z']) - 1] +
                                                                [-1, 0, 1] + [max(df_iter_non_zero[feature + '_z']) + 1])
                df_iter_non_zero[feature + '_Alphabecket'] = '-'
                for i in range(len(df_iter_non_zero[feature + '_bkt'].unique())):
                    df_iter_non_zero.loc[df_iter_non_zero[feature + '_bkt'] ==
                                         df_iter_non_zero[feature + '_bkt'].unique()[i], feature + '_Alphabecket'] = ascii_uppercase[i + 2]

                df_iter_non_zero = df_iter_non_zero.drop([feature + '_z', feature + '_bkt'], axis=1)

            df_iter = df_iter_zero.append(df_iter_non_zero).drop(columns=feature)
            df_features_z = df_features_z.merge(df_iter, how='left')

        df_features_z['GroupComb'] = df_features_z[[lt + '_Alphabecket' for lt in input_features]].apply(''.join,
                                                                                                       axis=1)
        df_features_z = df_features_z.drop([lt + '_Alphabecket' for lt in input_features], 1)
        group_assignment = df_features_z.groupby('GroupComb', as_index=False).agg(count=('delvry_plant', 'count'))
        group_assignment.loc[group_assignment['count'] < 3, 'count'] = -1

        group_assignment = pd.concat([group_assignment[group_assignment['count'] != -1].reset_index(drop=True).reset_index().drop('count', 1).rename(columns={'index': 'Group'}),
                                     group_assignment[group_assignment['count'] == -1].rename(columns={'count': 'Group'}).reset_index(drop=True)], axis=0)

        df_features_z = df_features_z.merge(group_assignment, how='left').drop(['GroupComb'], 1)
        df_features = df_features_z.copy()

        return df_features
    
    
    def find_clusters(self, df_geog_data, input_features): #function to cluster the routes by passing through algorithm
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(df_geog_data[input_features])
        
        #using the Ball tree algorithm
        dist_mat = NearestNeighbors(n_neighbors= len(input_features), algorithm= 'ball_tree').fit(scaled_data).kneighbors(scaled_data)[0]
        avg_distance= pd.DataFrame({'Inds': list(range(scaled_data.shape[0])), 'Values': sorted(np.mean(dist_mat, axis= 1))})
        
        # epsilon = avg_distance.Val[KneeLocator(avg_distance['Inds'], avg_distance['Values'], S= 20, curve= 'convex', direction= 'increasing').knee] #taking index of the value where the curve breaks
        
        knee_locator = KneeLocator(avg_distance['Inds'], avg_distance['Values'], S=20, curve='convex', direction='increasing')
        epsilon = avg_distance['Values'][knee_locator.knee]

        
        model = DBSCAN(eps= epsilon, min_samples= len(input_features))
        model.fit(scaled_data)
        pred = model.fit_predict(scaled_data)
        df_geog_data['group'] = pred
        
        return df_geog_data
    
    
    def remove_features(self, df_feat):
        df_feat = df_feat.drop(columns= ['nh_per','sh_per','hilly_per','plain_per','total_restriction_time'])
        return df_feat
    
    def clustered_dataframe(self, df_clean_no_geog_columns, df_features_with_cluster):
        df_clustered = pd.merge(df_clean_no_geog_columns, df_features_with_cluster, on= ['delvry_plant', 'city_code'], how= 'left')
        return df_clustered
    
    def creating_clustering_slabs(self, df_clustered):
        df_clustered['clustering_slab'] = ((df_clustered['lead']//self.clustering_slab) * self.clustering_slab).astype(int).astype(str) + '-' + (((df_clustered['lead'] + self.clustering_slab)//self.clustering_slab) * self.clustering_slab).astype(int).astype(str)
        return df_clustered
    
    def format_df_clustered(self, df_clustered):
        df_clustered['quantity'] = np.ceil(df_clustered['quantity'])
        df_clustered['ptpk'] = round(df_clustered['ptpk'], 2)
        df_clustered['group'] = np.where(df_clustered['group'] == -1, 18277, df_clustered['group'])
        df_clustered['group'] = df_clustered['group'].apply(lambda x: get_column_letter(x+1))
        df_clustered['group'] = np.where(df_clustered['group'] == 'ZZZ', 'MISC', df_clustered['group'])
        df_clustered['subgroup'] = (df_clustered['integration_flag'])*(2**2) + (df_clustered['cluster_flag'])*(2**1) + (df_clustered['union_flag'])*(2**0)
        df_clustered['group_combination'] = df_clustered['group'] + df_clustered['subgroup'].astype(str)
        df_clustered['median_ptpk'] = round(df_clustered.groupby(['clustering_slab', 'group_combination', 'truck_type', 'integration_flag', 'cluster_flag', 'union_flag'])['ptpk'].transform('median'),2)
        
        return df_clustered
    
    def compare_ptpk(self, df_clustered):
        df_clustered['optimized'] = np.where(df_clustered['ptpk'] <= df_clustered['median_ptpk'], 'ALready Optimized', 'Need Optimization')
        
        return df_clustered
        
    def predicted_freight(self, df_clustered):
        df_clustered['predicted_base_freight'] = df_clustered['median_ptpk'] * df_clustered['lead']
        return df_clustered
    
    def monthly_impact(self, df_clustered):
        df_clustered['monthly_impact'] = np.where(((df_clustered['median_ptpk'] < df_clustered['ptpk']) & (df_clustered['group_combination'] != 'MISC')),((df_clustered['ptpk']- df_clustered['median_ptpk'])* df_clustered['lead'] * df_clustered['total_quantity']),0)/3
        return df_clustered
    
    def monthly_quantity(self, df_clustered):
        df_clustered['monthly_quantity'] = df_clustered['total_quantity']/3
        return df_clustered
    
    def source_types(self, df_clustered):
        out = df_clustered[['group_combination', 'clustering_slab', 'truck_type', 'Unit Code']].drop_duplicates()
        out = out.groupby(['group_combination', 'clustering_slab', 'truck_type'])['Unit Code'].count().reset_index()
        out['plant_source_types'] = np.where(out['Unit Code']>1, 'Multiple', 'Single')
        out.drop(columns= ['Unit Code'], inplace= True)
        return out        
        
    
    def result_data(self):
        df_unit_merged = self.merge_data()
        df_unit_merged_cleaned = self.clean_data(df_unit_merged)
        
        input_features = ['nh_per', 'sh_per', 'plain_per', 'hilly_per', 'total_restriction_time']
        
        df_geog_data = self.calculate_z_scores(df_unit_merged_cleaned, input_features)
        
        df_features_with_cluster = self.find_clusters(df_geog_data, input_features)
        
        df_clean_no_geog_columns = df_unit_merged_cleaned.drop(columns= ['nh_per', 'sh_per', 'plain_per', 'hilly_per', 'total_restriction_time']).drop_duplicates()
        
        
        df_clustered = self.clustered_dataframe(df_clean_no_geog_columns, df_features_with_cluster)
        
        df_clustered = self.creating_clustering_slabs(df_clustered)
        
        df_clustered = self.format_df_clustered(df_clustered)
        
        df_clustered = self.compare_ptpk(df_clustered)
        
        df_clustered = self.predicted_freight(df_clustered)
        
        df_clustered = self.monthly_impact(df_clustered)
        
        df_clustered = self.monthly_quantity(df_clustered)
        
        out = self.source_types(df_clustered)
        
        final_result = pd.merge(df_clustered, out, on = ['group_combination', 'clustering_slab', 'truck_type'], how= 'left')
        
        final_result = final_result[['zone','Unit Code','delvry_plant','plant_name','city_code','city_desc','district_code', 'district_desc', 'depot_code', 'depot_desc',
                                   'region_code', 'region_desc', 'i2_taluka', 'i2_taluka_desc','truck_type', 'quantity', 'total_quantity','lead', 'ptpk', 'direct_sto', 'type','integration_flag',
                                   'cluster_flag', 'union_flag', 'base_freight', 'plant_type','nh_per', 'sh_per', 'hilly_per',
                                   'plain_per', 'total_restriction_time', 'group', 'clustering_slab',
                                   'median_ptpk', 'optimized', 'group_combination', 'predicted_base_freight',
                                   'Remarks(Taluka Analysis)',
                                   'MonthlyAvgImpact(Taluka Analysis)','plant_source_types']]
        
        return final_result
        
if __int__ =='__main__':   
   

    df = pd.read_csv(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\Plant_City_Truck_3Month_dispatch.csv")
    unit = pd.read_excel(r"C:\Users\nikhil.upadhyay-v\Downloads\SED\Unit Master.xlsx")
    clustering_slab = 10
    obj = Clustering_Routes(clustering_slab, df, unit) #object of the class Clustering_Routes
    
    res = obj.result_data() #final results
    res.to_csv('clustering output.csv')
        
        
        
        
        