
"""
download population, race and ethnicity variables 
from 2010 decennial census 
    SF1 Tables: P5, P6, P7, P10, P11
and 2015-2019 5YR ACS
    Tables:  B03002, B02008, B02009, B02010, B02011, B02012, B02013, B29001
    
state: NC

prerequisites: county fips crosswalk for state

"""

import cenpy as c
import pandas as pd
import geopandas as gpd
import os

##### SETUP
state_name = 'NC'
state_fips = '37'

cwd = os.getcwd()
cwd
os.chdir('D:/GIS data/{0}'.format(state_name))

# get county crosswalk
state_cross = pd.read_csv('D:/GIS data/county_fips/{0}_county_cross.csv'.format(state_name.lower()))
state_cross['fips'] = state_cross['fips'].map(lambda x: str(x).zfill(3)) 

# specify counties
state_counties = list(state_cross['fips'])

# set census API key
c.set_sitekey("41c56c0bf2ba46ee4d1551002447cfff9ab92d5b", overwrite=True)
# c.set_sitekey("censusapikey", overwrite=True)

############## ------------ CENSUS BLOCKS, DEC 10 ------------ ##############

# specify block shapefile if needed
in_shp = './Census/tl_2020_{0}_tabblock10/tl_2020_{0}_tabblock10.shp'.format(state_fips)

# specify output files
#out_csv = './Census/{0}_blocks_dec10.csv'.format(state_name)
out_shp = './Shapefiles/{0}_blocks_dec10.shp'.format(state_name)


# specify variable subset

# P5: population of each race alone, nonhispanic, hispanic
census_vars1 = ['P005001', # total 
                'P005002', 'P005003', 'P005004', 'P005005', 'P005006','P005007', 'P005008', 'P005009', # nonhispanic
                'P005010', 'P005011', 'P005012', 'P005013', 'P005014', 'P005015', 'P005016', 'P005017'] # hispanic

#P6, P7: each race alone or in combination with other races
census_vars2 = ['P006002', 'P006003', 'P006004', 'P006005', 'P006006', 'P006007', # any ethnicity
                'P007003', 'P007004', 'P007005', 'P007006', 'P007007', 'P007008',  # nonhispanic
                'P007010', 'P007011', 'P007012', 'P007013', 'P007014', 'P007015'] # hispanic

#P10: VAP for each race alone, any ethnicity
# additional BVAP alone or in combination with other races
census_vars3 = ['P010001', # total VAP
                'P010003','P010004','P010005','P010006','P010007','P010008', 'P010009', # each race alone / 2 or more
                'P010011','P010016','P010017','P010018','P010019', # BVAP + 1 race
                'P010027','P010028','P010029','P010030','P010037','P010038','P010039','P010040','P010041','P010042', # BVAP + 2 races
                'P010048','P010049','P010050','P010051','P010052','P010053','P010058','P010059','P010060','P010061', # BVAP + 3 races
                'P010064','P010065','P010066','P010067','P010069', # BVAP + 4 races
                'P010071'] # BVAP + 5 races

#P11: VAP for each race alone, nonhispanic
# addditional NH BVAP alone or in combination with other races
census_vars4 = ['P011002', # HVAP
                'P011003', # NHVAP
                'P011005','P011006','P011007','P011008','P011009','P011010', 'P011011', # NH each race alone / 2 or more
                'P011013','P011018','P011019','P011020','P011021', # NH BVAP + 1 race
                'P011029','P011030','P011031','P011032','P011039','P011040','P011041','P011042','P011043','P011044', # NH BVAP + 2 races
                'P011050','P011051','P011052','P011053','P011054','P011055','P011060','P011061','P011062','P011063', # NH BVAP + 3 races
                'P011066','P011067','P011068','P011069','P011071', # NH BVAP + 4 races
                'P011073'] # NH BVAP + 5 races


census_vars_list = [census_vars1, census_vars2, census_vars3, census_vars4]


##### GET DATA

# # check out all codes available
# codes = c.explorer.available()
# DEC_codes = codes.loc[codes.index.str.contains('DECENNIAL')]['title']

# # explain dataset
# datasets = list(c.explorer.available(verbose=True).items())
# pd.DataFrame(datasets).head()
# dataset = 'DECENNIALSF12010'
# c.explorer.explain(dataset)

# set connection to survey / year
conn = c.remote.APIConnection('DECENNIALSF12010')

# # check out all variables in current connection
# test = conn.geographies
# test = conn.geographies['fips'].head(100)

# var = conn.variables
# print('Number of variables in', dataset, ':', len(var))
# conn.variables.head()

data_list = []

for vset in census_vars_list:
    data = pd.DataFrame(columns=vset + ['state', 'county', 'tract', 'block'])
    
    # make census requests and store information in df
    for county in state_counties:
        print(county)
        county_data = conn.query(cols=vset, geo_unit='block', geo_filter={'state':state_fips, 'county':county})
        data = data.append(county_data)
        
    data['geoid'] = data['state'] + data['county'] + data['tract'] + data['block']
    
    data_list.append(data)
 
    
#### IF USING EXISTING SHAPEFILE - JOIN TO GEOMETRY

# read in blocks
blocks = gpd.read_file(in_shp)

blocks.dtypes
blocks['GEOID10'].head()
data_list[0]['geoid'].head()

# merge shp and df 
for dset in data_list:
    print(len(blocks) == len(dset))
    blocks = blocks.merge(dset, left_on='GEOID10', right_on='geoid',how='left')

# rename
blocks.rename(columns = {
                          'P005001':'tot',
                          'P005002':'NHtot',
                          'P005003':'NHwhi_alo',
                          'P005004':'NHbla_alo',
                          'P005005':'NHnat_alo',
                          'P005006':'NHasi_alo',
                          'P005007':'NHpci_alo',
                          'P005008':'NHsor_alo',
                          'P005009':'NH2mo',
                          'P005010':'Htot',
                          'P005011':'Hwhi_alo',
                          'P005012':'Hbla_alo',
                          'P005013':'Hnat_alo',
                          'P005014':'Hasi_alo',
                          'P005015':'Hpci_alo',
                          'P005016':'Hsor_alo',
                          'P005017':'H2mo',
                          'P006002':'whi_com',
                          'P006003':'bla_com',
                          'P006004':'nat_com',
                          'P006005':'asi_com',
                          'P006006':'pci_com',
                          'P006007':'sor_com',
                          'P007003':'NHwhi_com',
                          'P007004':'NHbla_com',
                          'P007005':'NHnat_com',
                          'P007006':'NHasi_com',
                          'P007007':'NHpci_com',
                          'P007008':'NHsor_com',
                          'P007010':'Hwhi_com',
                          'P007011':'Hbla_com',
                          'P007012':'Hnat_com',
                          'P007013':'Hasi_com',
                          'P007014':'Hpci_com',
                          'P007015':'Hsor_com',
                          'P010001':'totVAP',
                          'P010003':'whi_alo_VAP',
                          'P010004':'bla_alo_VAP',
                          'P010005':'nat_alo_VAP',
                          'P010006':'asi_alo_VAP',
                          'P010007':'pci_alo_VAP',
                          'P010008':'sor_alo_VAP',
                          'P010009':'2mo_VAP',
                          'P011002':'HVAP',
                          'P011003':'NHVAP',
                          'P011005':'NHwhi_alo_VAP',
                          'P011006':'NHbla_alo_VAP',
                          'P011007':'NHnat_alo_VAP',
                          'P011008':'NHasi_alo_VAP',
                          'P011009':'NHpci_alo_VAP',
                          'P011010':'NHsor_alo_VAP',
                          'P011011':'NH2mo_VAP'
                          }, inplace=True)   



                          
# create 1 race alone vars (P3)
blocks['whi_alo'] = blocks['NHwhi_alo'] + blocks['Hwhi_alo']
blocks['bla_alo'] = blocks['NHbla_alo'] + blocks['Hbla_alo']
blocks['nat_alo'] = blocks['NHnat_alo'] + blocks['Hnat_alo']
blocks['asi_alo'] = blocks['NHasi_alo'] + blocks['Hasi_alo']
blocks['pci_alo'] = blocks['NHpci_alo'] + blocks['Hpci_alo']
blocks['sor_alo'] = blocks['NHsor_alo'] + blocks['Hsor_alo']
blocks['2mo'] = blocks['NH2mo'] + blocks['H2mo']


# create BVAP alone or in combination with any other race 
blocks['bla_com_VAP'] = blocks['P010011'] + blocks['P010016'] + blocks['P010017'] + blocks['P010018'] + blocks['P010019'] + \
    blocks['P010027'] + blocks['P010028'] + blocks['P010029'] + blocks['P010030'] + \
    blocks['P010037'] + blocks['P010038'] + blocks['P010039'] + blocks['P010040'] + blocks['P010041'] + blocks['P010042'] + \
    blocks['P010048'] + blocks['P010049'] + blocks['P010050'] + blocks['P010051'] + blocks['P010052'] + blocks['P010053'] + \
    blocks['P010058'] + blocks['P010059'] + blocks['P010060'] + blocks['P010061'] + \
    blocks['P010064'] + blocks['P010065'] + blocks['P010066'] + blocks['P010067'] + blocks['P010069'] + blocks['P010071']  
    
# create NH BVAP alone or in combination with any other race 
blocks['NHbla_com_VAP'] = blocks['P011013'] + blocks['P011018'] + blocks['P011019'] + blocks['P011020'] + blocks['P011021'] + \
    blocks['P011029'] + blocks['P011030'] + blocks['P011031'] + blocks['P011032'] + \
    blocks['P011039'] + blocks['P011040'] + blocks['P011041'] + blocks['P011042'] + blocks['P011043'] + blocks['P011044'] + \
    blocks['P011050'] + blocks['P011051'] + blocks['P011052'] + blocks['P011053'] + blocks['P011054'] + blocks['P011055'] + \
    blocks['P011060'] + blocks['P011061'] + blocks['P011062'] + blocks['P011063'] + \
    blocks['P011066'] + blocks['P011067'] + blocks['P011068'] + blocks['P011069'] + blocks['P011071'] + blocks['P011073']  


# drop unncessary columns
cols = blocks.columns

blocks.drop(columns=census_vars3[8:], inplace=True)
blocks.drop(columns=census_vars4[9:], inplace=True)

blocks.drop(columns=['state_x', 'county_x', 'tract_x', 'block_x', 'geoid_x', 
                            'state_y', 'county_y', 'tract_y', 'block_y', 'geoid_y'], inplace=True)

cols = blocks.columns

blocks[cols[16:]] = blocks[cols[16:]].astype(float)

# write to file    
blocks.to_file(out_shp)


#### IF SHAPEFILES ARE NEEDED - GET TIGER DATA

# # check available tiger datasets
# c.tiger.available()

# # set map service
# conn.set_mapservice('tigerWMS_Census2010')

# # available map layers - layer 18 is blocks
# conn.mapservice.layers
# conn.mapservice.layers[18]

# # set up gpd
# geodata = gpd.GeoDataFrame()

# # request block geographies by county
# for county in state_counties:
#     print(county)
#     county_geodata = conn.mapservice.query(layer=18, where=('STATE={0} AND COUNTY={1}'.format(state_fips, county)))
#     geodata = geodata.append(county_geodata)
    
# # blocks from entire state - doesn't work because of 100000 block limit
# # this will work for higher level geographies
# #geodata = conn.mapservice.query(layer=18, where='STATE=37')

# # preview geodata
# geodata.iloc[:5, :5]
# geodata.dtypes
# geodata['GEOID'].head(10)
# data['geoid'].head(10)

# # join
# joined_data = geodata.merge(data, left_on='GEOID', right_on='geoid', how='left')
# joined_data.iloc[:5, -5:]

# # rename columns and drop 
# joined_data.drop(columns=['geoid', 'state', 'county', 'tract', 'block'], inplace=True)

# # save shapefile
# joined_data.to_file(out_shp)


############## ------------ BLOCK GROUPS, ACS 19 ------------ ##############


# specify block group shapefile if needed
in_shp = './Census/tl_2019_{0}_bg/tl_2019_{0}_bg.shp'.format(state_fips)

out_csv = './Census/{0}_bg_acs19.csv'.format(state_name)
out_shp = './Shapefiles/{0}_bg_acs19.shp'.format(state_name)

# specify variable subset
acs_vars = ['B03002_001E', # total
            'B03002_002E', 'B03002_003E', 'B03002_004E', 'B03002_005E', # nonhispanic
            'B03002_006E', 'B03002_007E', 'B03002_008E', 'B03002_009E', # nonhispanic
            'B03002_012E', 'B03002_013E', 'B03002_014E', 'B03002_015E', # hispanic
            'B03002_016E', 'B03002_017E', 'B03002_018E', 'B03002_019E', # hispanic
            'B02008_001E', 'B02009_001E', 'B02010_001E', 'B02011_001E', 'B02012_001E', 'B02013_001E', # each race alone in combination
            'B29001_001E'] # total VAP

pop_vars = ['P001001'] # 2010 pop for disaggregation

##### GET DATA

# # check out all codes available
# codes = c.explorer.available()

# # check out codes for ACS 5Yr 2019 only
# ACS_codes = codes.loc[codes.index.str.contains('5Y2019')]['title']

# # explain dataset
# datasets = list(c.explorer.available(verbose=True).items())
# pd.DataFrame(datasets).head()
# dataset = 'ACSDT5Y2019'
# c.explorer.explain(dataset)

# set connection to survey / year
conn = c.remote.APIConnection('ACSDT5Y2019')

# # check out the geographic filter requirements
# test = conn.geographies
# test = conn.geographies['fips'].head(100)

# # check out all variables in current ACS connection
# var = conn.variables
# print('Number of variables in', dataset, ':', len(var))
# conn.variables.head()

# # find list of variables in relevant table
# acs_vars = list(conn.varslike('B03002').index)

# geo_unit and geo_filter are both necessary arguments for the query() function. 
# geo_unit specifies the scale at which data should be taken. geo_filter then 
# creates a filter to ensure too much data is not downloaded 

# set up empty df
data = pd.DataFrame(columns=acs_vars + ['state', 'county', 'tract', 'block group'])

# query current ACS connection with columns, filter and geofilter settings
data = conn.query(cols=acs_vars, geo_unit='block group:*', 
                  geo_filter={'state':'{0}'.format(state_fips),
                              'county':'*',
                              'tract':'*'})

# create geoid
data['geoid'] = data['state'] + data['county'] + data['tract'] + data['block group']

# set to float
data.dtypes
data[acs_vars] = data[acs_vars].astype(float)

# rename
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

#### get total population for 2010 BGs

# set connection to survey / year
conn = c.remote.APIConnection('DECENNIALSF12010')

# set up empty df
data_pop = pd.DataFrame(columns=pop_vars + ['state', 'county', 'tract', 'block group'])

# query current ACS connection with columns, filter and geofilter settings
data_pop = conn.query(pop_vars, geo_unit='block group:*', 
                      geo_filter={'state':'{0}'.format(state_fips),
                                  'county':'*',
                                  'tract':'*'})

# create geoid
data_pop['geoid'] = data_pop['state'] + data_pop['county'] + data_pop['tract'] + data_pop['block group']

# set to float
data_pop.dtypes
data_pop[pop_vars] = data_pop[pop_vars].astype(float)

# rename
data_pop.rename(columns={'P001001':'tot10'}, inplace=True)

#### combine all BG data
data = data.merge(data_pop, on='geoid', how='left')

# write csv to file
data.to_csv(out_csv)


#### IF USING EXISTING SHAPEFILE - JOIN TO GEOMETRY

# read in blocks
bgs = gpd.read_file(in_shp)
bgs.dtypes
bgs['GEOID'].head()

data['geoid'].head()

# check for same number of blocks in both
print(len(bgs) == len(data))

# merge shp and df
bgs = bgs.merge(data, left_on='GEOID', right_on='geoid', how='left')
bgs.dtypes

# drop unncessary columns
bgs.drop(columns=['state_x', 'county_x', 'tract_x', 'block group_x',
                          'geoid', 'state_y', 'county_y', 'tract_y', 'block group_y'], inplace=True)

# write to file    
bgs.to_file(out_shp)

#### IF SHAPEFILES ARE NEEDED - GET TIGER DATA

# c.tiger.available()

# conn.set_mapservice('tigerWMS_ACS2019')

# # print layers available
# conn.mapservice.layers
# conn.mapservice.layers[10]

# # set up gpd
# geodata = gpd.GeoDataFrame()

# # request block groups (layer 10) for state
# geodata = conn.mapservice.query(layer=10, where='STATE={0}'.format(state_fips))

# # preview geodata
# geodata.iloc[:5, :5]
# geodata.dtypes

# # merge shp and df
# joined_bgs = geodata.merge(data, left_on='GEOID', right_on='geoid', how='left')

# # rename columns and drop  - table B03002
# joined_bgs.drop(columns=['B03002_021E', 'B03002_020E', 'B03002_002E', 
#                           'B03002_011E', 'B03002_010E',
#                           'NAME', 'GEO_ID', 'state', 'county', 
#                           'tract', 'block group', 'geoid'], inplace=True)
# # save to file
# joined_data.to_file(out_shp)

