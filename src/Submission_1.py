from packages.boto_service import (S3_REGION,
                                   S3_BUCKET,
                                   QUERY_FILE_C1,
                                   VALIDATION_FILE_C1,
                                   init_s3_client,
                                   s3_select_query,
                                   s3_stream_to_dataframe)
import geopandas as gpd
import pandas as pd
from loguru import logger
import timeit
import os


def validation_run():
    # init client
    s3_client = init_s3_client(S3_REGION)

    # Challenge File: SELECT FROM city-hex-polygons-8-10.geojson WHERE resolution = 8
    logger.info(f"**********************  GET CHALLENGE DATA FOR SELECT FROM city-hex-polygons-8-10.geojson WHERE resolution = 8") 
    s3_select_query_result = s3_select_query(s3_client, S3_BUCKET, QUERY_FILE_C1, 8)
    # Parse Stream data to geopandas dataframe
    start = timeit.default_timer()
    stream_df = s3_stream_to_dataframe(s3_select_query_result)
    end = timeit.default_timer()
    logger.info(f"Challenge Data :Convert list of dicts into geopandas dataframe process completed. Time Taken: {end - start}s")     
    
    # Validation File : SELECT FROM city-hex-polygons-8.geojson
    logger.info(f"**********************  GET VALIDATION DATA FOR SELECT FROM city-hex-polygons-8.geojson")
    if os.path.exists(f"downloaded_files/{VALIDATION_FILE_C1.replace('geojson', 'csv')}"):
        validation_df = pd.read_csv(f"downloaded_files/{VALIDATION_FILE_C1.replace('geojson', 'csv')}", index_col=False)
        validation_df = gpd.GeoDataFrame(validation_df)
    else:
        s3_select_query_result = s3_select_query(s3_client, S3_BUCKET, VALIDATION_FILE_C1)
        # Parse Stream data to geopandas dataframe
        start = timeit.default_timer()
        validation_df = s3_stream_to_dataframe(s3_select_query_result)
        end = timeit.default_timer()
        logger.info(f"Validation Data : Convert list of dicts into geopandas dataframe process completed. Time Taken: {end - start}s")   
        # Save validation file for next run
        os.makedirs("downloaded_files")
        validation_df.to_csv(f"downloaded_files/{VALIDATION_FILE_C1.replace('geojson', 'csv')}", index=False)
    
    # Validate Stream Data aginst validation file
    logger.info(f"**********************  START VALIDATION OF CHALLENGE DATA AGAINT VALIDATION DATA")
    start = timeit.default_timer()
    compare_df = stream_df['index'].compare(validation_df['index'])
    end = timeit.default_timer()
    logger.info(f"Validate Stream Data aginst validation file process completed. Time Taken: {end - start}s")     

    return compare_df

if __name__ == '__main__':
    start = timeit.default_timer()

    # Run the validation process
    compare_df = validation_run()

    # Evaluate result
    if compare_df.empty:
        status = "Validation Succeeded!"
    else:
        status = "Validation Failed!"
    
    # Return process time
    end = timeit.default_timer()
    logger.info(f"Challenge 1 Completed. {status} Time Taken: {end - start}s")
