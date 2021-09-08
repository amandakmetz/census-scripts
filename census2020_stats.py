
#%% imports

import pandas as pd
import geopandas as gpd
import urllib
import zipfile
import time
import os

#%% read state fips crosswalk
state_fips = pd.read_csv('D:/GIS data/county_fips/state_fips.csv')
state_fips['name'] = state_fips['name'].str.replace(' ', '_')
state_fips['fips'] = state_fips['fips'].map(lambda x:str(x).zfill(2))


fips_list = list(state_fips['fips'])         # FIPS CODES
abbrev_list = list(state_fips['abbrev'])     # ABBREVIATIONS
name_list = list(state_fips['name'])         # NAMES

#%% set up working directory to location of state data folders
os.chdir('D:/GIS data/')
cwd = os.getcwd()
cwd

#%% loop through state data and calculate stats 
# for i in range(0, len(abbrev_list)):
for i in range(0, 3):

    cur_abbrev = abbrev_list[i]
    cur_name = name_list[i]
    cur_fips = fips_list[i]
    
    print(cur_name, cur_abbrev)
    
    block_shp = gpd.read_file('./{0}/Shapefiles/{0}_block_dec20.shp'.format(cur_abbrev))
    bg_shp = gpd.read_file('./{0}/Shapefiles/{0}_bg_dec20.shp'.format(cur_abbrev))
    tract_shp = gpd.read_file('./{0}/Shapefiles/{0}_tract_dec20.shp'.format(cur_abbrev))
    cousub_shp = gpd.read_file('./{0}/Shapefiles/{0}_cousub_dec20.shp'.format(cur_abbrev))
    county_shp = gpd.read_file('./{0}/Shapefiles/{0}_county_dec20.shp'.format(cur_abbrev))
    cd_shp = gpd.read_file('./{0}/Shapefiles/{0}_cd_dec20.shp'.format(cur_abbrev))
    place_shp = gpd.read_file('./{0}/Shapefiles/{0}_place_dec20.shp'.format(cur_abbrev))
    vtd_shp = gpd.read_file('./{0}/Shapefiles/{0}_vtd_dec20.shp'.format(cur_abbrev))
    
    print(state_fips.loc[i])
    state_fips.loc[i, 'block_count'] = len(block_shp)
    state_fips.loc[i, 'bg_count'] = len(bg_shp)
    state_fips.loc[i, 'tract_count'] = len(tract_shp)
    state_fips.loc[i, 'cousub_count'] = len(cousub_shp)
    state_fips.loc[i, 'county_count'] = len(county_shp)
    state_fips.loc[i, 'cd_count'] = len(cd_shp)
    state_fips.loc[i, 'place_count'] = len(place_shp)
    state_fips.loc[i, 'vtd_count'] = len(vtd_shp)
  
    
#%% write to file

# state_fips.to_csv('./county_fips/state_fips_stats.csv')

#%%