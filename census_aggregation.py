
"""
Daily Kos Census Data Collection Project 
Data Aggregation Template
----------------------------------------
7/6/21

- disaggregates ACS 2019 variables from block groups to blocks
- assigns districts from block equivalency files to blocks
- groups blocks by districts and sums ACS data

"""

import pandas as pd
import geopandas as gpd
import maup
import os
import time

def checkGeo(gdf):
    '''
    
    Parameters
    ----------
    gdf : geoDataFrame
        geoDataFrame to check for topology errors

    Returns
    -------
    None.

    ''' 
    # tests for valid geometries
    if gdf['geometry'].is_valid.all():
        print('no invalid geometries')
    else:
        print('warning: invalid geometries')
        
    # tests for empty geometries
    if gdf['geometry'].is_empty.any():
        print('warning: empty geometries')
    else:
        print('no empty geometries')
        
    # tests for missing geometries
    if gdf['geometry'].isna().any():
        print('warning: missing geometries')
    else:
        print('no missing geometries')
        
    # if gdf.geometry.duplicated().any():
    #     print('warning: duplicated geometry')
    # else:
    #     print('no duplicated geometries')
        
    # test for duplicated indices
    if gdf.index.duplicated().any():
        print('warning: duplicated index')
    else:
        print('no duplicated indices')

    
##### SETUP

# set current state name and working directory for files
state_name = 'GA'
state_fips = '13'

cwd = os.getcwd()
cwd
os.chdir('D:/GIS data/{0}'.format(state_name))

#### IMPORTS

# # blocks
# blocks = gpd.read_file('./Shapefiles/{0}_blocks_dec10.shp'.format(state_name))

# # drop all demographic variables for demo
# dropcols = list(blocks.columns)[16:76]
# blocks.drop(columns=dropcols, inplace=True)

# blocks.to_file('D:/Github/census-scripts/Shapefiles/{0}_blocks10.shp'.format(state_name))

# blocks with only 2010 population
blocks = gpd.read_file('D:/Github/census-scripts/Shapefiles/{0}_blocks10.shp'.format(state_name))

blocks.head()

# block groups
bgs = gpd.read_file('./Shapefiles/{0}_bg_acs19.shp'.format(state_name))

# block equivalency file
block_eq = pd.read_csv('D:/Github/census-scripts/Shapefiles/Georgia VRA §5 12th Block Equivalency.csv')

#### DATA PREP

# check crs
blocks.crs
bgs.crs

# check geometry
blocks['geometry'] = blocks.geometry.buffer(0)
bgs['geometry'] = bgs.geometry.buffer(0)

checkGeo(bgs)
checkGeo(blocks)

# blocks have only 2010 population 
# block groups have 2010 population + other demographic variables
# we will use 2010 population to weight the disaggregation of other variables
# get the subset of variables that we want to move from block groups to blocks
bg_cols = list(bgs.columns)[12:36]

# find the unique GEOID for block groups and make it the index of bgs
# we need to do this in order to use the maup.prorate function later on
bgs.dtypes
bgs['GEOID'] = bgs['GEOID'].map(lambda x:str(x).zfill(12))  # convert column to string and add leading zeroes just in case
bgs.set_index('GEOID', inplace=True)

# create a new column in blocks that corresponds to the block group geoids
# the block group GEOID is the same as the first 12 digits of the block GEOID
blocks['BGGEOID'] = blocks['GEOID10'].map(lambda x:(str(x).zfill(15))[:12])  
blocks['BGGEOID'].head()
blocks['BGGEOID'].nunique()  # check to make sure we have the same number of unique BGGEOIDs as rows in bgs

# convert population variables to float just in case
blocks.dtypes
bgs.dtypes
bgs[bg_cols] = bgs[bg_cols].astype(float)

#### DISAGGREGATION

# now we disaggregate the block group data to blocks based on proportion of 2010 population
# create an assignment - this is a series in the blocks df that corresponds to the index column of bgs
assignment = blocks['BGGEOID']
# create weights - the proportion of each block's pop to the corresponding block group's total pop
weights = blocks.tot / assignment.map(bgs.tot10)
# create prorated values using the assignment, specifying the columns to prorate, and the weights 
prorated = maup.prorate(assignment, bgs[bg_cols], weights)
# assign the prorated values to the corresponding columns in blocks
blocks[bg_cols] = prorated

#### TESTING

# preview the disaggregated values
test_subset = blocks.head(100)

# check total population at block level and block group level
blocks['tot19'].sum()       #10403651
bgs['tot19'].sum()          #10403847
blocks['tot'].sum()         #9687653
bgs['tot10'].sum()          #9687653

# apply formula for median income across geographic levels

#### GROUPING BY PLAN DISTRICTS
block_eq.dtypes
block_eq['BlockID'] = block_eq['BlockID'].map(lambda x:str(x).zfill(15))
block_eq.set_index('BlockID', inplace=True)

# check for unique block IDs in the block equivaleny file
block_eq.index.nunique()

# join the block equivalency with the district numbers to the corresponding blocks
blocks_join = blocks.merge(block_eq, left_on='GEOID10', right_index=True, how='inner')

# preview joined blocks
test_subset = blocks_join.head(100)

# group data by district numbers with the sum function
# this will only sum numeric columns and ignore anything that can't be summed
blocks_grouped = blocks_join.groupby('District').sum()

# if we want to retain the district shapes, we have to use the dissolve function
blocks_grouped_gdf = blocks_join.dissolve(by='District', aggfunc='sum')

# write csv and gdf to file
blocks_grouped.to_csv('./Scratch/{0}_blocks_grouped.csv'.format(state_name))
blocks_grouped_gdf.to_file('./Scratch/{0}_blocks_grouped.shp'.format(state_name))












