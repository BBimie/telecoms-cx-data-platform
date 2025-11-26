import os

def get_existing_files(client, bucket, folder):
    """
    Returns a set of filenames (without extension) that already exist in the destination bucket to prevent duplicating log fetching.
    """
    existing_files = set()
    paginator = client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=folder):
        if 'Contents' in page:
            for obj in page['Contents']:
                filename = os.path.basename(obj['Key'])
                file_stem = os.path.splitext(filename)[0]
                existing_files.add(file_stem)
                
    return existing_files