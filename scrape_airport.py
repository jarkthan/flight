import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Define the URL for the data source
url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'

# Load the data into a Pandas dataframe
df = pd.read_csv(url, header=None, names=['AirlineID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active'])

# Define the connection string and blob information
connectionString = 'DefaultEndpointsProtocol=https;AccountName=dwthanakornrung;AccountKey=k3+ju9o06l6FeVP722RJdumOGmS4Bxha4XNKucGxcZjhtyxh4AUGY/J2zenszJZknVprkqI5lmAO+ASt2uh9qQ==;EndpointSuffix=core.windows.net'
container_name = 'data'
blob_name = 'raw_airport.csv'

# Create a blob service client and blob client to interact with the blob
blob_service_client = BlobServiceClient.from_connection_string(connectionString)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(blob_name)

# Convert the dataframe to CSV and upload to the blob
blob_client.upload_blob(df.to_csv(index=False), overwrite=True)