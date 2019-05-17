import requests
from collections import OrderedDict
from operator import itemgetter
import geojson
from shapely.geometry import asShape, GeometryCollection

def get_mosaic_time_dict_from_series_id(mosaic_series_id, observed_start, observed_end):
    """Get dict of {mosaic_name: start_date} mappings from Mosaic API using mosaic series abstraction

    Args:
        mosaic_series_id: str,
        observed_start: datetime string, start of observed TOI
        observed_start: datetime string, end of observed TOI
    Returns:
        List of mosaic name strings
    """

    r = requests.get(
        'http://mosaics.prod.planet-labs.com/internal/series/{}/mosaics/'.format(mosaic_series_id))
    r.raise_for_status()  # stop here if series not found
    mosaic_series = r.json()
    sorted_mosaic_series = sorted(mosaic_series['mosaics'], key=itemgetter(u'first_acquired'))

    mosaic_dict = OrderedDict()
    for mosaic_item in sorted_mosaic_series:
        if (mosaic_item['first_acquired'] >= observed_start) and (mosaic_item['last_acquired'] <=
                                                                  observed_end):
            mosaic_dict[mosaic_item['name']] = mosaic_item['first_acquired']

    return mosaic_dict

def match_aoi_input(aoi):
    """
    Determine whether an AOI input is a filepath or a geojson string.
    Inputs:
        aoi: filepath or geojson string
    :return:
    Returns: geojson string
    """
    if aoi.endswith('json'):
        with open(aoi) as fp:
            return geojson.load(fp)
    else:
        return geojson.loads(aoi)

def geojson_to_shape(gj_dict):
    """Convert GeoJSON dict to a shapely object, with support for higher-level types

    Note: geojson coords are directly pass through to shapely object, and there
    is no handling of coordinate systems or wrapping logic.

    Args:
        gj_dict - geojson dict
    Returns:
        shapely object, specific type depends on geojson type
    """
    if isinstance(gj_dict, geojson.feature.Feature):
        return asShape(gj_dict['geometry'])
    elif isinstance(gj_dict, geojson.feature.FeatureCollection):
        return GeometryCollection([asShape(feat['geometry']) for feat in gj_dict['features']])
    elif isinstance(gj_dict, geojson.geometry.GeometryCollection):
        return GeometryCollection([asShape(geom) for geom in gj_dict['geometries']])
    else:  # Polygon, LineString, etc.
        return asShape(gj_dict)