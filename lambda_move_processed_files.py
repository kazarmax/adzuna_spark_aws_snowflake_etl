import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def get_s3_object_keys(bucket_name, folder_prefix):
    """
    List all object keys in the specified S3 folder.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_prefix (str): Folder prefix in the S3 bucket (should end with a slash).

    Returns:
        list: A list of object keys in the specified folder.
    """
    
    try:
        logger.info(f"Listing objects in bucket: {bucket_name}, folder: {folder_prefix}")
        
        # List objects with the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        
        if 'Contents' in response:
            objects = response['Contents']
            logger.info(f"Found {len(objects)} objects in the folder.")
            
            # Extract object keys
            object_keys = [obj['Key'].split("/")[-1] for obj in objects if obj != folder_prefix]
            return object_keys
        else:
            logger.info(f"No objects found in folder: {folder_prefix}")
            return []

    except Exception as e:
        logger.error(f"Error occurred while listing objects: {str(e)}")
        return []

def delete_s3_object(bucket_name, object_key):
    try:
        # Delete the object
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"File {object_key} deleted successfully from bucket {bucket_name}.")

    except Exception as e:
        logger.error(f"Error occurred while deleting file: {str(e)}")
        raise

def move_s3_object(bucket, source_key, destination_key):
    logger.info(f"Started moving {source_key} to 'processed' folder in s3 ...")
    try:
        # Copy the object
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=destination_key
        )
        logger.info(f"File copied from {source_key} to {destination_key} successfully.")
        delete_s3_object(bucket, source_key)

    except Exception as e:
        logger.error(f"Error occurred while moving file: {str(e)}")
        raise

def lambda_handler(event, context):
    # Moving processed source data files to a different folder
    bucket = "adzuna-etl-project-maksim"
    to_process_folder = "raw_data/to_process/"
    processed_folder = "raw_data/processed/"

    object_keys = get_s3_object_keys(bucket, to_process_folder)
    for object_key in object_keys:
        source_key = to_process_folder + object_key
        destination_key = processed_folder + object_key
        move_s3_object(bucket, source_key, destination_key)