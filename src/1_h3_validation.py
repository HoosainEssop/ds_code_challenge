from packages.boto_service import (S3_REGION,
                                   S3_BUCKET,
                                   init_s3_client,
                                   download_s3_object,
                                   s3_select_query,
                                   s3_stream_to_dict)
import geopandas as gpd

def validation_run():
    # Download File and create dataframe
    s3_client = init_s3_client(S3_REGION)
    obj_downloaded = download_s3_object(s3_client, S3_BUCKET, 'city-hex-polygons-8.geojson')
    if obj_downloaded:
        validate_df = gpd.read_file('./city-hex-polygons-8.geojson', driver="GeoJSON")
        

    # Select stream data and create dataframe
    s3_select_query_result = s3_select_query(s3_client, S3_BUCKET, 'city-hex-polygons-8-10.geojson', 8)
    df_dict = s3_stream_to_dict(s3_select_query_result)
    stream_df = gpd.GeoDataFrame.from_records(df_dict)
    stream_df['index'] = stream_df['properties'].apply(lambda x : x['index'])


    validation_df = stream_df['index'].compare(validate_df['index'])

    return validation_df

if __name__ == '__main__':
    validation_df = validation_run()
    if validation_df.empty:
        print("Validation Succeeded!")
    else:
        raise Exception("Validation Failed!")