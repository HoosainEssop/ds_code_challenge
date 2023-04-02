from packages.boto_service import (S3_REGION,
                                   S3_BUCKET,
                                   QUERY_FILE_C2,
                                   VALIDATION_FILE_C2,
                                   VALIDATION_FILE_C1,
                                   init_s3_client,
                                   download_s3_object)
import geopandas as gpd
import pandas as pd
from loguru import logger
import os
import botocore
import gzip
import matplotlib.pyplot as plt
import h3
import timeit

def get_obj(s3_client: botocore.client, source_filename: str, target_filename: str):
        obj_downloaded = download_s3_object(s3_client, S3_BUCKET, source_filename, target_filename)

        if obj_downloaded:
            if not os.path.exists(f'./downloaded_files'):
                os.makedirs('downloaded_files')
            if not os.path.exists(f"downloaded_files/{target_filename}"):
                os.rename(f"{target_filename}", f"downloaded_files/{target_filename}")
            return True
        else:
            return False

def read_gzip_df(filename: str):
    with gzip.open(filename) as f_:
        _df = pd.read_csv(f_, index_col=False)
        if 'Unnamed: 0' in _df.columns:
            del(_df['Unnamed: 0'])
    return _df
    

if __name__ == '__main__':
    start = timeit.default_timer()

    s3_client = init_s3_client(S3_REGION)

    # Get Challenge file
    logger.debug("Get Challenge file")
    source_filename = QUERY_FILE_C2
    target_filename = QUERY_FILE_C2
    file_returned =  get_obj(s3_client, source_filename, target_filename)
    sr_df = read_gzip_df(f"downloaded_files/{QUERY_FILE_C2}")

    # Read Validation file from challenge 1
    logger.debug("Read Validation file from challenge 1")
    source_filename = VALIDATION_FILE_C1
    target_filename = VALIDATION_FILE_C1
    file_returned =  get_obj(s3_client, source_filename, target_filename)
    poly_8_df =  gpd.read_file(f"downloaded_files/{VALIDATION_FILE_C1}")

    # Get Validation file
    logger.debug("Download Validation file for challenge 2")
    source_filename = VALIDATION_FILE_C2
    target_filename = VALIDATION_FILE_C2
    file_returned =  get_obj(s3_client, source_filename, target_filename)
    sr_hex_df = read_gzip_df(f"downloaded_files/{VALIDATION_FILE_C2}")

    # Overlay polygons to get an understanding wrt the error tolerance
    logger.debug("Download Validation file for challenge 2")
    Res_SIZE = 8
    hex_col = f'hex{Res_SIZE}'

    # calucalte the h3 index's for the lat lon pair at a given resolution
    logger.debug("Transform the Service Request Data")
    sr_df[hex_col] = sr_df.apply(lambda x: h3.geo_to_h3(x.latitude, x.longitude, Res_SIZE), axis = 1)

    # remove records where lat and lon is null, resulting in a 0 hex value
    sr_df = sr_df[sr_df[hex_col] != '0'].copy()

    # Create a frame that contains the count of each hex index
    sr_ind_cnt_df = sr_df.groupby(hex_col).size().to_frame('cnt').reset_index()

    #find center of hex for visualization
    sr_ind_cnt_df['lat'] = sr_ind_cnt_df[hex_col].apply(lambda x: h3.h3_to_geo(x)[0])
    sr_ind_cnt_df['lng'] = sr_ind_cnt_df[hex_col].apply(lambda x: h3.h3_to_geo(x)[1])

    # Create the geodataframe for the aggregated service requests
    sr_ind_cnt_gdf = gpd.GeoDataFrame(sr_ind_cnt_df, geometry=gpd.points_from_xy(sr_ind_cnt_df['lng'], sr_ind_cnt_df['lat']))

    # Create a geodataframe for the service requests
    sr_gdf = gpd.GeoDataFrame(sr_df, geometry=gpd.points_from_xy(sr_df['longitude'], sr_df['latitude']))

    # Overlay polygons to get an understanding wrt the error tolerance
    # fig, ax = plt.subplots(figsize=(20, 15))
    # sr_ind_cnt_gdf.plot(ax=ax, column='cnt', alpha=0.4)
    # # sr_gdf.plot(ax=ax, color="red", alpha=0.4)
    # poly_8_df.plot(ax=ax, color="blue", alpha=0.4)
    # plt.show()

    # Set an Error Threshold of 1% as this caters for any data capture issues, rounding errors.
    # and is significantly less than what we have encounted in the above plot, where three missing records were identified
    # 2 missing matches at point (-34.044257, 18.774378) and 1 at point (-33.904955, 18.723060)
    ERROR_THRESH = 1

    # Left join the sr_gdf to the poly_8_df to get all matches
    logger.debug("Merge the service request data to the city-hex-polygons-8.geojson contents, each service request is assigned to a single H3 resolution level 8 hexagon")
    gdf_merged = sr_gdf.merge(poly_8_df, left_on=hex_col, right_on='index', how='left')

    # calculate error records percentage
    logger.debug("Validate Merged data, compare assigned H3 resolution and Calculate the error percentages")
    sr_len = len(sr_gdf)
    matches_len = len(gdf_merged[gdf_merged['index'].notnull()])
    error_count = sr_len - matches_len
    error_perc = (error_count / sr_len) * 100
    logger.success(f"service request dataset has {sr_len} records, the matched merge dataframe has {error_count} less records at {matches_len} records, the error percentage is {error_perc:f}%")
    # Assert that we did not receive more errors than the threshold allows
    assert error_perc <= ERROR_THRESH, f"The error percentage exceeds the threshold of {str(ERROR_THRESH)}"

    if error_perc > 0.0:
        # Compare against validation file
        sr_hex_val_df = sr_hex_df[['notification_number', 'reference_number', 'latitude', 'longitude', 'h3_level8_index']]
        sr_hex_val_df = sr_hex_val_df[sr_hex_val_df['h3_level8_index'] != '0']

        gdf_merged_comp = gdf_merged[['notification_number', 'reference_number', 'latitude', 'longitude', 'index']]
        
        sr_hex_val_df.columns = sr_hex_val_df.columns.str.replace('notification_number', 'h3_notification_number')
        result_comp_df = sr_hex_val_df.merge(gdf_merged_comp
                                ,left_on=['h3_notification_number','h3_level8_index']
                                ,right_on = ['notification_number','index']
                                ,how = 'left')
        
        missing_notification_matches = result_comp_df[result_comp_df['notification_number'].isnull()][['h3_notification_number', 'h3_level8_index']].values.tolist()
        print("Validation Completed!")
        if len(missing_notification_matches) > 0:
            logger.warning(f"The Following Notification Numbers with it's respective Index could not be reconciled against the validation file: {missing_notification_matches}")

        notifications = [i[0] for i in missing_notification_matches]
        indexes = [i[1] for i in missing_notification_matches]

        found_notif_sr = sr_gdf[sr_gdf['notification_number'].isin(notifications)]['notification_number'].to_list()
        found_index_poly_8_df = poly_8_df[poly_8_df['index'].isin(indexes)]['index'].to_list()

        missing_notifications = set(notifications) - set(found_notif_sr)
        missing_indexes = set(indexes) - set(found_index_poly_8_df)

        if len(missing_notifications) + len(missing_indexes) > 0:
            logger.info("The mismatch could be due to:")
        if len(missing_notifications) > 0:
            logger.info(f"\t - The following notification numbers were in the SR dataset: {set(notifications) - set(found_notif_sr)}")
        if len(missing_indexes) > 0:
            logger.info(f"\t - The following indexes were not in the poly_8 dataset: {set(indexes) - set(found_index_poly_8_df)}")
    else:
        logger.success(f"All Notification Numbers and it's respective Index could be reconciled against the validation file!")
    
    end = timeit.default_timer()
    logger.success(f"Challenge 2 Completed. Time Taken: {end - start}s")