# Submission for Data Engineering Position
This is a submission for the data engineering position 


## Installation

### Requirements

- This requires python 3.8 or greater 
- An internet connection to access external data sources
- `secrets.json` file with dummy AWS credentials, it can be found [here](https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json).
 

Install python requirements found in requirements.txt

```bash
pip install -r requirements.txt
```

Download/Create secrets.json file from [file located at](https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json).
- do this manually or run the script as below

```bash
python get_secrets.py
```






# Task 1 
Does the following
- retrieves data from AWS using S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson.
- Downloads the city-hex-polygons-8.geojson from aws.
- Validates the result set against the city-hex-polygons-8.geojson file to validate your work.
- logs time taken for operations 
### Usage

```bash
python task_1.py
```

### Notes
- For optimisation in comparison and validation of results `geopandas.GeoDataFrame.compare` was used
.

# Task 2 
Does the follwing 
- Retrieves sr.csv.gz and sr_hex.csv from AWS
- calculates the error rate based on missing latitude and longitude values in sr.csv
- calculates the h3_index for each service request in sr.csv
- validates the sr.csv with h3_index information against sr_hex.csv

### Usage 

```bash
python task_2.py
```

### Notes
- The h3_index was created using the uber h3 python library.
- For optimisation in comparison and validation of results `geopandas.GeoDataFrame.compare` was used
.

# Task 5
- Retrieve the Bellville South centroid from arcgis opendata
- Creates a subset of the sr_hex.csv with values that are 1 minute away from bellville south 
- Retrieves the wind direction and speed from a publicly hosted ods file.
- Cleans the ods data and puts it into a dataframe.
### Usage 

```bash
python task_5.py
```

### Notes
- Bellville South data is retrieved from the arcgis opendata platform at https://odp-cctegis.opendata.arcgis.com/datasets/cctegis::official-planning-suburbs/about