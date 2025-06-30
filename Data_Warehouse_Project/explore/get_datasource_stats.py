import boto3

def get_total_size(bucket, prefix):
    s3 = boto3.client("s3", region_name="us-west-2")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    total_size = 0
    file_count = 0

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                total_size += obj["Size"]
                file_count += 1

    return total_size, file_count


# Initialize
bucket_name = "udacity-dend"

# Get sizes
song_size, song_count = get_total_size(bucket_name, "song_data/")
log_size, log_count = get_total_size(bucket_name, "log_data/")

# Summary
total_size_bytes = song_size + log_size
total_size_mb = total_size_bytes / (1024 * 1024)

print(f"Song Data: {song_count} files, {song_size / (1024 * 1024):.2f} MB")
print(f"Log Data: {log_count} files, {log_size / (1024 * 1024):.2f} MB")
print(f"Total: {song_count + log_count} files, {total_size_mb:.2f} MB")
