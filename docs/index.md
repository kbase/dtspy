# dtspy: a Python client for the Data Transfer System (DTS)

The `dtspy` Python client allows you to use the [DTS](https://kbase.github.io/dts/)
to search for files and transfer them between several organizations supported
by the Department of Energy (DOE) Biological and Environmental Research (BER)
program:

* [JGI Data Portal (JDP)](https://data.jgi.doe.gov/)
* [DOE National Microbiome Data Collaborative (NMDC)](https://microbiomedata.org/)
* [DOE Biology Knowledgebase(KBase)](https://www.kbase.us)
* More coming soon!

You can use `dtspy` to create a client that connects to a DTS server, and use
that client to

* search for data files within databases supported by the above organizations,
  obtaining their canonical IDs and metadata identifying their provenance
* request the transfer of one or more of these data files between databases by
  using canonical file IDs

## Prerequisites for Using `dtspy`

To use the DTS Python client, you need

* Python 3.12
* an [ORCID account](https://orcid.org/register)
* a [KBase developer account](https://docs.kbase.us/development/create-a-kbase-developer-account) with a
  [developer token](https://kbase.github.io/kb_sdk_docs/tutorial/3_initialize.html#set-up-your-developer-credentials)

DTS uses KBase's authentication server, so the token associates your DTS client
with your KBase account, allowing you to transfer files to it.

## Installation

We recommend installing `dtspy` in a [virtual environment](https://docs.python.org/3/library/venv.html)
alongside any tools or workflows you use to analyze the data you're working with.
You can install `dtspy` using `pip` (or `pip3`):

```
pip install dtspy
```

## Quickstart

Below is an example describing how to create a DTS client connected to a server,
search for files within the JGI Data Portal, and print their metadata.


```
import dts
import os

# to avoid putting your developer token in your Python scripts, store it
# in an environment variable like `DTS_KBASE_DEV_TOKEN`
token = os.getenv('DTS_KBASE_DEV_TOKEN')

# connect to the DTS server
dts_client = dts.Client(api_key = token,
                        server = "https://lb-dts.staging.kbase.us")

# search the JGI Data Portal for files related to prochlorococcus, a small
# marine cyanobacteria
files = dts_client.search(database = 'jdp',
                          query = 'prochlorococcus')
print(files)

```
