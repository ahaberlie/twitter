# -*- coding: utf-8 -*-
"""
Created on Tue Dec 09 09:14:13 2014

@author: Thomas Pingel
"""
#%%

import pandas as pd

def load_twitterlog(fn,query=''):
    
    print "loading " + fn
    
    col_names = ['time','tweet_id','user_id','lat','lon','user_location','tweet_text']

    # Read in file
    df = pd.read_csv(fn,delimiter='\t',names=col_names,skiprows=1)
    
    # Convert time to numeric; sometimes this field gets corrupted
    df['time'] = df['time'].convert_objects(convert_numeric=True)
    df['lon'] = df['lon'].convert_objects(convert_numeric=True)
    df['lat'] = df['lat'].convert_objects(convert_numeric=True)
    df['tweet_id'] = df['tweet_id'].convert_objects(convert_numeric=True)
    df['user_id'] = df['user_id'].convert_objects(convert_numeric=True)

    # Keep only rows with lat/lon
    has_lat_lon = pd.notnull(df.loc[:,'lat']) 
    df = df.ix[has_lat_lon,:]
    
    # Eliminate rows outside of the bounding box
    bounding_box = [-125.001106, 24.949320, -66.932640, 49.590370] # CONUS
    df = df.loc[df.loc[:,'lon']>=bounding_box[0]]
    df = df.loc[df.loc[:,'lon']<=bounding_box[2]]
    df = df.loc[df.loc[:,'lat']>=bounding_box[1]]
    df = df.loc[df.loc[:,'lat']<=bounding_box[3]]
    
    # Make a query; remove other rows; This just matches, but doesn't remove
    if query!='':
        matches_query = df.loc[:,'tweet_text'].str.contains(query,na=False)
        df = df.ix[matches_query,:]
    
    # Convert to datetime
    df['time'] = pd.to_datetime(df['time'],utc=True,unit='s')
   
    
    
    return df