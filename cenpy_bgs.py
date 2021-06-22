
"""
Download census data from the 2019 ACS
Specify a state and run to download all ACS data for a single state at the block group level
Change ACS variables to variables in the Daily Kos tables 

"""


import cenpy as c
import pandas as pd
import geopandas as gpd
import os

##### STATE SETUP

# change these to current state
state_name = 'GA'
state_fips = '13'

# set working directory - change to current folder with shapefiles for state
cwd = os.getcwd()
cwd
os.chdir('D:/GIS data/{0}'.format(state_name))

# get county crosswalk for looping through counties and fips codes
state_cross = pd.read_csv('D:/GIS data/county_fips/{0}_county_cross.csv'.format(state_name.lower()))
state_cross['fips'] = state_cross['fips'].map(lambda x: str(x).zfill(3)) 

# specify counties in current state
state_counties = list(state_cross['fips'])

# set census API key - change to your API key
c.set_sitekey("censusapikey", overwrite=True)
# c.set_sitekey("41c56c0bf2ba46ee4d1551002447cfff9ab92d5b", overwrite=True)


############## ------------ BLOCK GROUPS, ACS 19 ------------ ##############

#### IMPORTS AND SETUP

# specify block group shapefile if needed
in_shp = './Census/tl_2019_{0}_bg/tl_2019_{0}_bg.shp'.format(state_fips)

# set output location for csv and shapefile
out_csv = './Census/{0}_bg_acs19.csv'.format(state_name)
out_shp = './Shapefiles/{0}_bg_acs19.shp'.format(state_name)

# specify ACS variable subset here if you know exactly which variables you need
# these are all ACS variables
# acs_vars = ['B03002_001E', # total
#             'B03002_002E', 'B03002_003E', 'B03002_004E', 'B03002_005E', # nonhispanic
#             'B03002_006E', 'B03002_007E', 'B03002_008E', 'B03002_009E', # nonhispanic
#             'B03002_012E', 'B03002_013E', 'B03002_014E', 'B03002_015E', # hispanic
#             'B03002_016E', 'B03002_017E', 'B03002_018E', 'B03002_019E', # hispanic
#             'B02008_001E', 'B02009_001E', 'B02010_001E', 'B02011_001E', 'B02012_001E', 'B02013_001E', # each race alone in combination
#             'B29001_001E'] # total VAP

# only decennial census variable - 2010 total population
# this is used as the link between populations in block groups and blocks
pop_vars = ['P001001'] 

##### GET DATA - 2019 ACS

# shows you all possible survey codes available through the Census API
# we are only interested in the 2019 5YR ACS, so we filter to only those
codes = c.explorer.available()
ACS_codes = codes.loc[codes.index.str.contains('5Y2019')]['title']

# set dataset to the 2019 5YR ACS - Detailed Tables
# and get a description of the dataset
dataset = 'ACSDT5Y2019'
c.explorer.explain(dataset)

# set connection to the 2019 5YR ACS - Detailed Tables
conn = c.remote.APIConnection('ACSDT5Y2019') # Detailed Tables

# check out all variables in current ACS connection
var = conn.variables
print('Number of variables in', dataset, ':', len(var))
conn.variables.head()

# find list of variables in each relevant table
# here we set up a separate list for each table we need
# if you need additional variables, specify them in a new list 
# or use the varslike() function to search for all variables in a table
acs_vars1 = list(conn.varslike('B03001').index) # hispanic or latino by specific origin
acs_vars2 = list(conn.varslike('B15003').index) # educational attainment for adults over 25
acs_vars3 = list(conn.varslike('C15002H').index) # educational attainment for adults over 25 (nonhispanic white only)


# setting up an empty dataframe before we query the API for the data
# this helps us control the columns in our output data
data = pd.DataFrame(columns=acs_vars + ['state', 'county', 'tract', 'block group'])

# query current ACS connection - this retrieves the ACS data for all block groups in a state
data = conn.query(cols=acs_vars,  # cols arguments tells the API which variables to retrieve
                  geo_unit='block group:*',  # geo_unit tells the API to get all block groups
                  geo_filter={'state':'{0}'.format(state_fips),  # geo_filter set to only current state
                              'county':'*',   # all counties within state
                              'tract':'*'})  # all tracts within state

# create geoid column so we can match block group data to the shapefile
data['geoid'] = data['state'] + data['county'] + data['tract'] + data['block group']

# set all variables to float for disaggregation
# normally they download as strings
data.dtypes
data[acs_vars] = data[acs_vars].astype(float)

# rename columns to match variable descriptions
# this will need to change depending on which variables you download
data.rename(columns = {
                        'B03002_001E':'tot19',
                        'B03002_002E':'NHtot19',
                        'B03002_003E':'NHwhi_alo19',
                        'B03002_004E':'NHbla_alo19',
                        'B03002_005E':'NHnat_alo19',
                        'B03002_006E':'NHasi_alo19',
                        'B03002_007E':'NHpci_alo19',
                        'B03002_008E':'NHsor_alo19',
                        'B03002_009E':'NH2mo19',
                        'B03002_012E':'Htot19',
                        'B03002_013E':'Hwhi_alo19',
                        'B03002_014E':'Hbla_alo19',
                        'B03002_015E':'Hnat_alo19',
                        'B03002_016E':'Hasi_alo19',
                        'B03002_017E':'Hpci_alo19',
                        'B03002_018E':'Hsor_alo19',
                        'B03002_019E':'H2mo19',
                        'B02008_001E':'whi_com19',
                        'B02009_001E':'bla_com19',
                        'B02010_001E':'nat_com19',
                        'B02011_001E':'asi_com19',
                        'B02012_001E':'pci_com19',
                        'B02013_001E':'sor_com19',
                        'B29001_001E':'totVAP19'
                          }, inplace=True)   

#### GET DATA - 2010 DECENNIAL - TOTAL POP

# switch connection to decennial census
conn = c.remote.APIConnection('DECENNIALSF12010')

# setting up an empty dataframe before we query the API for the data
data_pop = pd.DataFrame(columns=pop_vars + ['state', 'county', 'tract', 'block group'])

# query current ACS connection - this retrieves the Decennial 2010 data for all block groups in a state
data_pop = conn.query(cols=pop_vars, geo_unit='block group:*',  # changed columns to pop_vars
                      geo_filter={'state':'{0}'.format(state_fips),
                                  'county':'*',
                                  'tract':'*'})

# create geoid column so we can match block group data to the shapefile
data_pop['geoid'] = data_pop['state'] + data_pop['county'] + data_pop['tract'] + data_pop['block group']

# set all variables to float for disaggregation
data_pop.dtypes
data_pop[pop_vars] = data_pop[pop_vars].astype(float)

# rename columns to match variable descriptions
data_pop.rename(columns={'P001001':'tot10'}, inplace=True)

#### MERGE ALL BLOCK GROUP DATA

# merge ACS data with DECENNIAL 2010 data
data = data.merge(data_pop, on='geoid', how='left')

# write csv to file without spatial information
data.to_csv(out_csv)

#### IF USING EXISTING SHAPEFILE - JOIN TO GEOMETRY

# read in block group shapefile
bgs = gpd.read_file(in_shp)


# check column data types and make sure they match 
# make sure that both geoid columns have appropriate leading zeros 
# make sure that both geoid columns are object (string) type
bgs.dtypes
bgs['GEOID'].head()
data['geoid'].head()

# check for same number of blocks in both
print(len(bgs) == len(data))

# merge all block group data with the spatial data
bgs = bgs.merge(data, left_on='GEOID', right_on='geoid', how='left')

# preview all the columns to find anything we don't want to keep
bgs.dtypes

# drop unncessary/duplicate columns
bgs.drop(columns=['state_x', 'county_x', 'tract_x', 'block group_x',
                          'geoid', 'state_y', 'county_y', 'tract_y', 'block group_y'], inplace=True)

# save final block group shapefile with all ACS + Decennial Census data
bgs.to_file(out_shp)


