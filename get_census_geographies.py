
"""

download geographies from the 2011 tiger line census files
unzip in folders

https://www2.census.gov/geo/tiger/TIGER2011/PLACE/tl_2011_01_place.zip	
https://www2.census.gov/geo/tiger/TIGER2011/COUSUB/tl_2011_01_cousub.zip

- place (done)
- county subdivisions (done)
- blocks (done)
- state shape (done)

- project all to 3857 (state shape)
- and create difference of state shape (diff) place

"""

# importing required modules
import pandas as pd
import geopandas as gpd
import urllib
import zipfile
import time
import os

# read crosswalk
state_fips = pd.read_csv('D:/GIS data/county_fips/state_fips.csv')

state_fips['fips'] = state_fips['fips'].map(lambda x:str(x).zfill(2))
fips_list = list(state_fips['fips'])
abbrev_list = list(state_fips['abbrev'])

cwd = os.getcwd()
cwd
os.chdir('D:/GIS data/')

#download 2011 places
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2011/PLACE/tl_2011_{0}_place.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2011_{0}_place/".format(abbrev_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
        
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)
    

#download 2011 county subdivisions
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2011/COUSUB/tl_2011_{0}_cousub.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2011_{0}_cousub/".format(abbrev_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)
    
#download 2010 counties
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_{0}_county10.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2010_{1}_county10/".format(abbrev_list[i], fips_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)

#download 2010 states
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2010/STATE/2010/tl_2010_{0}_state10.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2010_{1}_state10/".format(abbrev_list[i], fips_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)

#download 2019 block groups
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2019/BG/tl_2019_{0}_bg.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2019_{0}_bg/".format(abbrev_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)
        

#download 2020 block groups
for i in range(0, len(abbrev_list)):
    print (abbrev_list[i], fips_list[i])

    url = "https://www2.census.gov/geo/tiger/TIGER2020/BG/tl_2020_{0}_bg.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2020_{0}_bg/".format(abbrev_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)
        
        
t0 = time.time()      
# download 2010 blocks
# averages ~3.5 minutes per state
for i in range(42, len(abbrev_list)): # SD - VA
    print (abbrev_list[i], fips_list[i])
    
    url = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK/tl_2020_{0}_tabblock10.zip".format(fips_list[i])
    extract_dir = "./{0}/Census/tl_2020_{0}_tabblock10/".format(abbrev_list[i])

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
        
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(extract_dir)
t1 = time.time()
elapsed = t1-t0        




t0 = time.time()
# for each state, project places and county subdivisions and state shapes to EPSG 3857 pseudo mercator
for i in range(41,50) :
    st_name = abbrev_list[i]
    st_fips = fips_list[i]
    
    print(st_name, st_fips)
    
    print("read files")
    state_dir = "D:/GIS data/{0}".format(abbrev_list[i])
    place = gpd.read_file("{0}/tl_2011_{1}_place.shp".format(state_dir,fips_list[i]))
    cousub = gpd.read_file("{0}/tl_2011_{1}_cousub.shp".format(state_dir,fips_list[i]))
    st_shape = gpd.read_file("{0}/{1}_TL_2019_STATE_proj.shp".format(state_dir, abbrev_list[i]))
    
    print("change crs")
    place = place.to_crs(st_shape.crs)
    cousub = cousub.to_crs(st_shape.crs)
    
    place.to_file("{0}/tl_2011_{1}_place_proj.shp".format(state_dir,fips_list[i]))
    cousub.to_file("{0}/tl_2011_{1}_cousub_proj.shp".format(state_dir,fips_list[i]))
    
    print("spatial difference")
    place_out = gpd.overlay(st_shape, place, how="difference")
    place_all = place.append(place_out)
    place_all.to_file("{0}/tl_2011_{1}_place_all.shp".format(state_dir,fips_list[i]))
    
t1 = time.time()
total = t1-t0
  





      