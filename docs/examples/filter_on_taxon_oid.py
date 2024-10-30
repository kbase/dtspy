# This script searches the JGI Data Portal for files related to a given 
# IMG taxon OID, printing out any related metadata.

import argparse
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

# fetch search arguments from the command line
parser = argparse.ArgumentParser(
    prog='filter_on_taxon_oid.py',
    description='''a small script that uses dtspy to search the JGI Data Portal
for data associated with a given IMG taxon OID''',
)
parser.add_argument('--taxon_oid',
    default = '2708742931',
    help = 'an IMG taxon OID with which search results should be associated',
    required = False,
    type = int,
)
args = parser.parse_args()

# do the search
results = dts_client.search(database = 'jdp',
                            query = f'{args.taxon_oid}',
                            specific = jdp_params)
print(results)
