This stage of the pipeline is responsible for extracting the Census data we are interested in from the public datasets in google big query, cleaning the data, and then pushing it back to our master table in Google Big Query where it will be used for visualization and modeling. 

Data sets used:
American Community Survey data in Google Big Query 
https://www.census.gov/programs-surveys/acs
https://console.cloud.google.com/marketplace/product/united-states-census-bureau/acs

Resources:
https://billpetti.github.io/2020-10-19-using-bigquery-to-join-census-location-data-geocoding-sql/

Notably, there is no 2020 Census data at the 1 yr level, there is a 5 year aggregate dataset for 2020. This will be an opporunity for our pipeline to fill in the gaps.

Requires the Google Cloud Client
https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-usage-python
https://console.cloud.google.com/welcome?walkthrough_id=bigquery--python-client-library

You must install the client with pip for this to work, and set up your authentication details, following the instructions in the link above.
```
pip3 install google-cloud-bigquery
```

There is a service account I have created you can use. You need a json file with an auth key and to declare an environment variable. PM me for details.
Once you have the json file with your key, do this:
```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
```

Then you can run the client libraries in python