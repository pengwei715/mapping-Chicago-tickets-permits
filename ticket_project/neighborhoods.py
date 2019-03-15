import geopandas as geo_pd
import pandas as pd
import shapely
from sodapy import Socrata

ZIPCODES_ID = 'unjd-c2ca'
NEIGHS_ID = 'y6yq-dbs2'

def import_geometries(ds_id, proj=None):
    '''
    Imports the shapefiles from a given data store on the City of Chicago data
    portal and retuns a geopandas dataframe linking geometries to different
    attributes.

    Inputs:
    ds_id (str): the data set identifier
    
    Returns: geodf from ds_id
    '''
    if not proj:
        proj = {'init': 'epsg:4326'}

    client = Socrata('data.cityofchicago.org', 'SB7994tcuBpSSczrQvMx9N0Uy',
                     username="benfogarty@uchicago.edu", password='d5Nut6LrCHL&')

    files = client.get(ds_id)
    df = pd.DataFrame(files)
    df['the_geom'] = df.the_geom.apply(shapely.geometry.shape)
    df = geo_pd.GeoDataFrame(df, geometry='the_geom')
    df.crs = proj
    df.drop(['shape_area', 'shape_len'], axis=1, inplace=True)
    
    return df

def link_neighs_zips(zipcodes, neighborhoods):
    '''
    Returns a dictionary linking neighborhoods to a list of zipcodes
    intersecting that neighborhood. Each neighborhood may be linked to multiple
    zipcodes and vice versa as neighborhoods may intersect multiple zipcodes and
    vice versa.

    Inputs:
    zipcodes (GeoPandas GeoDataFrame): describes the boundaries of zipcode areas
        within the Chicago city limits
    neighborhoods (GeoPandas GeoDataFrame): describes the boundaries of 98
        neighborhoods defined withing the Chicago city limits

    Returns: dictionary
    '''
    link = geo_pd.sjoin(neighborhoods, zipcodes, how='inner', op='intersects')
    neighs_zips_dict = {}
    for neighborhood in list(neighborhoods['pri_neigh']):
    	mask = link['pri_neigh'] == neighborhood
    	neighs_zips_dict[neighborhood] = list(link[mask]['zip'])

    return neighs_zips_dict

def convert_to_geodf(df, long_col, lat_col, proj=None):
    '''
    Converts a regular pandas dataframe to a geopandas dataframe, based on
    coordinated in the regular pandas dataframe.

    Inputs:
        df (Pandas DataFrame): the dataframe to convert
        long_col (str): the name of the column containing longitude coordinates
        lat_col (str): the name of the column containing latitude coordinates
        proj (dict): the projection for the GeoDataFrame coordinates
    
    Returns (geopandas GeoDataFrame)
    '''
    if not proj:
        proj = {'init': 'epsg:4326'}

    df = df.loc[(df[long_col].notna() & df[lat_col].notna())]    
    df['coordinates'] = list(zip(df[long_col], df[lat_col]))
    df.loc[:,'coordinates'] = df.coordinates.apply(shapely.geometry.Point)
    geodf = geo_pd.GeoDataFrame(df, geometry='coordinates')
    geodf.crs = proj
    return geodf

def find_neighborhoods(geo_df, neighborhoods):
    '''
    Performs a spatial join to link the entries in a GeoDataFrame with their
    respective neighborhoods

    Inputs:
        geo_df (GeoPandas GeoDataFrame): the geodataframe to link with
            neighborhoods
        neighborhoods (GeoPandas GeoDataFrame): a GeoDataFrame containing all
            the neighborhoods

    Returns: (GeoPandas GeoDataFrames)
    '''
    geo_df = geo_df.to_crs(neighborhoods.crs)
    merged = geo_pd.sjoin(geo_df, neighborhoods, how='left', op='within', 
    					  rsuffix='_neig')
    return merged
