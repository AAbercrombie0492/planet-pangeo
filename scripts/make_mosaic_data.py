"""
Script that generates Mosaic and Mosaic Quad Metadata datasets from a mosaic series ID, an AOI, and a TOI

Example Usage
python scripts/california_mosaics.py \
    --aoi data/california.geojson \ 
    --observed-start 2018-01-01T00:00:00.000000 \
    --observed-end 2019-02-01T00:00:00.000000 \
    --mosaic-series-id 431b62a0-eaf9-45e7-acf1-d58278176d52 
"""
from planet import api
from planet.api import filters
import geojson
import pandas as pd
import json
# import geopandas as gpd
import shapely.geometry
from copy import deepcopy
import subprocess
from utils import get_mosaic_time_dict_from_series_id, match_aoi_input, geojson_to_shape
import datetime
import os
import argparse
import requests
from tqdm import tqdm

PL_API_KEY = os.environ.get('PL_API_KEY')

MOSAICS_API_BASE_URL = 'https://api.planet.com/basemaps/v1/mosaics/'

# flatten list function
flatten = lambda l: [item for sublist in l for item in sublist]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--aoi',
        default='data/geojsons/san-francisco.geojson',
        help='GeoJSON AOI File')    
    parser.add_argument(
        '--mosaic-series-id',
        default='431b62a0-eaf9-45e7-acf1-d58278176d52',
        help='Mosaic Timeseries ID ; e.g. for global monthly series')
    parser.add_argument(
        '--observed-start',
        default='2018-01-01T00:00:00.000000',
        help='Timestamp string for beginning of observation window')
    parser.add_argument(
        '--observed-end', 
        default='2019-02-01T00:00:00.000000',
        help='Timestamp string for end of observation window')
    parser.add_argument(
        '--output-dir',
        default='data/mosaics',
        help='Directory where to save metadata outputs'
    )

    return parser.parse_args()

def main():
    # parse input args
    proc_args = parse_args()
    aoi_geojson = match_aoi_input(proc_args.aoi)
    aoi_geom = geojson_to_shape(aoi_geojson)
    aoi_bounds = aoi_geom.bounds
    mosaic_series_id = proc_args.mosaic_series_id
    observed_start = proc_args.observed_start
    observed_end = proc_args.observed_end
    output_dir = proc_args.output_dir

    mosaic_info_df = make_mosaic_info(mosaic_series_id, observed_start, observed_end)
    mosaic_info_df.to_csv("{}/mosaic_info.csv".format(output_dir))
    print("Mosaic Metadata saved to {}/mosaic_info.csv".format(output_dir))

    all_quads_df = make_quad_info_df(mosaic_info_df, aoi_geom)
    all_quads_df.to_csv('{}/quad_info.csv'.format(output_dir))
    print("Quad Metadata saved to {}/quad_info.csv".format(output_dir))

def make_quad_info_df(mosaic_info_df, aoi_geom):
    # Get Quads for each Mosaic
    quad_dataframes = []
    for i, row in tqdm(mosaic_info_df.iterrows(), desc='Getting Mosaic Quads'):
        mosaic_quad_data = generate_aoi_quads(aoi_geom, row['id'])
        mosaic_quad_records = flatten([_ for _ in mosaic_quad_data])
        mosaic_quads_df = pd.DataFrame.from_records(mosaic_quad_records)
        quad_dataframes.append(mosaic_quads_df)
    # Concatenate all mosaic quads into a master dataframe
    all_quads_df = pd.concat(quad_dataframes)
    return all_quads_df


def make_mosaic_info(mosaic_series_id, observed_start, observed_end):
    """
    Get Mosaic level Metadata
    """
    # Get collection of mosaics in TOI
    mosaic_time_dict = get_mosaic_time_dict_from_series_id(mosaic_series_id, observed_start, observed_end)

    # Get mosaic info
    mosaic_info_records = []
    for mosaic_name in tqdm(mosaic_time_dict.keys(), desc='Getting Mosaic Info'):
        # Construct bash command
        mosaic_info_cmd = [
            'planet',
            'mosaics',
            'info',
            mosaic_name
        ]
        # execute process and get Mosaic API response
        mosaic_info_proc  = subprocess.Popen(mosaic_info_cmd, stdout=subprocess.PIPE)
        mosaic_info, err = mosaic_info_proc.communicate()
        mosaic_info = json.loads(mosaic_info)['mosaics'][0]
        mosaic_info_records.append(mosaic_info)

    # Unpack nested attributes
    mosaic_info_df = pd.DataFrame.from_records(mosaic_info_records)
    mosaic_info_df['self_link'] = mosaic_info_df['_links'].apply(lambda x: x['_self'])
    mosaic_info_df['quads_link'] = mosaic_info_df['_links'].apply(lambda x: x['quads'])
    mosaic_info_df['tiles_link'] = mosaic_info_df['_links'].apply(lambda x: x['tiles'])
    mosaic_info_df['quad_size'] = mosaic_info_df['grid'].apply(lambda x: x['quad_size'])
    mosaic_info_df['resolution'] = mosaic_info_df['grid'].apply(lambda x: x['resolution'])

    return mosaic_info_df

def generate_aoi_quads(aoi_geom, mosaic_id):
    """
    Generator function that yields mosaic quad metadata
    """

    # Construct API query url 
    aoi_bounds = aoi_geom.bounds
    quads_url = 'https://api.planet.com/basemaps/v1/mosaics/{}/quads?api_key={}&bbox='.format(mosaic_id, PL_API_KEY)
    bbox = ','.join(map(str, aoi_bounds))
    quads_url += bbox

    # Get data
    result = requests.get(quads_url)
    mosaic_quad_data = result.json()

    # while paginating...
    while True:
        print(quads_url)
        
        for item in mosaic_quad_data['items']:
            quads = []
            # Only get data if quad bbox intersects aoi geometry
            box_geom = shapely.geometry.box(*item['bbox'])
            if aoi_geom.intersects(box_geom):
                q_data = {
                    "self_link": item['_links']['_self'],
                    "download_link": item['_links']['download'],
                    "items_link": item['_links']['items'],
                    "thumbnail": item['_links']['thumbnail'],
                    "bbox": item['bbox'],
                    "id": item['id'],
                    "percent_covered": item['percent_covered'],
                    "mosaic_id": mosaic_id,
                }
                quads.append(q_data)
            if quads: yield quads
        
        # Paginate if a _next link is present
        quads_url = mosaic_quad_data.get('_links', {}).get('_next')
        if not quads_url:
            break
        else:
            result = requests.get(quads_url)
            mosaic_quad_data = result.json()


if __name__ == "__main__":
    main()
