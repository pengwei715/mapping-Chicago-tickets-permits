import geopandas as geo_pd
import matplotlib.pyplot as plt
import descartes
import pandas as pd
import shapely

def read_shapefiles(filepath):
    '''
    Reads the neighborhood shapefiles into a geopandas dataframe

    Inputs:
    	filepath (str): the path to the neighborhood shapefiles

    Returns: geopandas dataframe describing neighborhoods
    '''
    return geo_pd.read_file(filepath)


def convert_to_geodf(df):
	'''
	Converts a regular pandas dataframe to a geopandas dataframe, based on
	coordinated in the regular pandas dataframe.

	Inputs:
		df (Pandas DataFrame): the dataframe to convert

	Returns (geopandas GeoDataFrame)
	'''
	df['coordinates'] = df.apply(lambda x: (x.xcoordinate, x.ycoordinate), axis=1)
	df = df[~df.xcoordinate.isna() & ~ df.ycoordinate.isna()].copy()
	df.loc[:,'coordinates'] = df['coordinates'].apply(shapely.geometry.Point)
	
	geodf = geo_pd.GeoDataFrame(df, geometry='coordinates')
	return geodf