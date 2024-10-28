# This script searches the JGI Data Portal for files related to a given 
# IMG taxon OID, printing out any related metadata.

import dts
import os

# connect to the DTS in NERSC Spin
token = os.getenv('DTS_KBASE_DEV_TOKEN')
dts_client = dts.Client(api_key = token,
                        server = "https://lb-dts.staging.kbase.us")

# add some JDP-specific search options
jdp_params = {
    'f': 'img_taxon_oid',  # filter on taxon OID
    'extra': 'project_id', # add project_id to returned metadata
}

# do the search
results = dts_client.search(database = 'jdp',
                            query = '2708742931',
                            specific = jdp_params)
print(results)

