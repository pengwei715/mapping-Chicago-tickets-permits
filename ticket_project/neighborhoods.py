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

def link_zips_neighs(zipcodes, neighborhoods):
    '''
    Returns an updated neighborhoods dataframe coontaining a zipcode column.
    Each neighborhood may be linked to multiple zipcodes and vice versa as
    neighborhoods may intersect multiple zipcodes and vice versa.

    Inputs:
    zipcodes (GeoPandas GeoDataFrame): describes the boundaries of zipcode areas
        within the Chicago city limits
    neighborhoods (GeoPandas GeoDataFrame): describes the boundaries of 98
        neighborhoods defined withing the Chicago city limits

    Returns:GeoPandas GeoDataFrame
    '''
    return geo_pd.sjoin(neighborhoods, zipcodes, how='inner', op='intersects')\
                 .filter(['pri_neigh', 'sec_neigh', 'zip', 'the_geom'])\
                 .drop_duplicates(subset=['pri_neigh', 'sec_neigh', 'zip'])

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
    merged = geo_pd.sjoin(geo_df, neighborhoods, how='left', op='within', rsuffix='_neig')
    return merged

def find_neighborhoods2(geo_df, neighborhoods, block_on_zip=False):
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
    if block_on_zip:
        zips = import_geometries(ZIPCODES_ID)
        neighborhoods = link_zips_neighs(zips, neighborhoods)
        output_geodf = geo_pd.GeoDataFrame(crs=neighborhoods.crs)
        for zipcode in set(zips['zip']):
            tickets_to_join = geo_df[geo_df['zipcode'] == zipcode]
            neighs_to_join = neighborhoods[neighborhoods['zip'] == zipcode]
            if not tickets_to_join.empty:
                concat = find_neighborhoods(tickets_to_join, neighs_to_join)
                output_geodf = geo_pd.GeoDataFrame( pd.concat([output_geodf, concat]), crs=output_geodf.crs)
    else:
        output_geodf = find_neighborhoods(geo_df, neighborhoods)

    return output_geodf
