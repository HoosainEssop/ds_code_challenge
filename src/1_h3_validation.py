from packages.boto_service import (S3_REGION,
                                   S3_BUCKET,
                                   init_s3_client,
                                   download_s3_object,
                                   s3_select_query,
                                   s3_stream_to_dict)
import geopandas as gpd
from loguru import logger
import timeit


def validation_run():
    # init client
    s3_client = init_s3_client(S3_REGION)

    # Select stream data and create dataframe
    s3_select_query_result = s3_select_query(s3_client, S3_BUCKET, 'city-hex-polygons-8-10.geojson', 8)

    # Parse Stream data to list of dicts
    df_dict = s3_stream_to_dict(s3_select_query_result)

    # Convert lst of dicts into geopandas dataframe
    start = timeit.default_timer()
    stream_df = gpd.GeoDataFrame.from_records(df_dict)
    stream_df['index'] = stream_df['properties'].apply(lambda x : x['index'])
    end = timeit.default_timer()
    logger.info(f"Convert list of dicts into geopandas dataframe process completed. Time Taken: {end - start}s")     
    
    # download validation file
    obj_downloaded = download_s3_object(s3_client, S3_BUCKET, 'city-hex-polygons-8.geojson')

    # Read Validation File
    start = timeit.default_timer()
    if obj_downloaded:
        validate_df = gpd.read_file('./city-hex-polygons-8.geojson', driver="GeoJSON")
    end = timeit.default_timer()
    logger.info(f"Read Validation file into geopandas dataframe process completed. Time Taken: {end - start}s")     
        
    # Validate Stream Data aginst validation file
    start = timeit.default_timer()
    validation_df = stream_df['index'].compare(validate_df['index'])
    end = timeit.default_timer()
    logger.info(f"Validate Stream Data aginst validation file process completed. Time Taken: {end - start}s")     

    return validation_df

if __name__ == '__main__':
    start = timeit.default_timer()

    # Run the validation process
    validation_df = validation_run()

    # Evaluate result
    if validation_df.empty:
        status = "Validation Succeeded!"
    else:
        status = "Validation Failed!"
    
    # Return process time
    end = timeit.default_timer()
    logger.info(f"Challenge 1 Completed. {status} Time Taken: {end - start}s")
