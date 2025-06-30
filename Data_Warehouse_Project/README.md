## Project summary
## How to run scripts

### Explore

```bash
aws s3 ls s3://udacity-dend/log-data/ --recursive --human-readable --summarize
aws s3 ls s3://udacity-dend/song-data/ --recursive --human-readable --summarize
aws s3 cp s3://udacity-dend/log_json_path.json .
```

To preview a specific file:

```bash
aws s3 cp s3://udacity-dend/log-data/<filename> - | head
```

aws s3 cp s3://udacity-dend/log-data/2018/11/2018-11-30-events.json - | head
aws s3 cp s3://udacity-dend/song-data/B/Z/G/TRBZGTJ128F4276373.json - | head

## terraform

redshift_endpoint = "sparkify-cluster.csp96d9mkp7i.us-west-2.redshift.amazonaws.com:5439"
redshift_iam_role_arn = "arn:aws:iam::978902700260:role/RedshiftS3AccessRole"

## File descriptions