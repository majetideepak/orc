#!/usr/bin/bash
mkdir -p data/nyc
(cd data/nyc; wget https://storage.googleapis.com/tlc-trip-data/2015/yellow_tripdata_2015-{11..12}.csv)
(cd data/nyc; gzip *.csv)
mkdir -p data/github
(cd data/github; wget http://data.githubarchive.org/2015-11-{01..15}-{0..23}.json.gz)