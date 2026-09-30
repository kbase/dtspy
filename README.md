# dtspy

![Tests](https://github.com/kbase/dtspy/actions/workflows/tests.yml/badge.svg)

Python client for the Data Transfer Service.

## Prior Art (fodder for agent tools)

Here we show code fragments for accomplishing various operations with the client.

### Extracting data from columns in a spreadsheet (e.g. IMG taxon OIDs)

```
def extract_columns(csv_file = None, column_names = []):
    with open(csv_file) as csvfile:
        reader = csv.reader(csvfile)
        indices = [-1 for name in column_names]
        columns = [[] for name in column_names]
        for row in reader:
            for i in range(len(column_names)):
                if indices[i] == -1: # header row
                    indices[i] = 0
                    while row[indices[i]] != column_names[i]:
                        indices[i] += 1
                else:
                    columns[i].append(row[indices[i]])
    return columns
```

### Searching for files by JGI Project IDs

NOTE: A JGI Project ID can be either an ITS AP or ITS SP identifier.

```
def map_project_ids_to_files(dts_client, project_ids: []str) -> dict[str, Any]:
    # The JDP allows us to string together multiple IDs into a query, but it's
    # a GET request, so we have to batch the IDs into separate queries to make
    # sure the total request length doesn't exceed the 4096 character limit.
    # For safety, we limit each request query to 1000 unencoded characters.
    queries = []
    query = ''
    for i, project_id in enumerate(project_ids):
        if len(query) == 0:
            query += project_id
        elif len(query) + 1 + len(project_id) <= 1000:
            query += f' | project_id
        else:
            queries.append(query)
            query = ''
    if len(query) > 0:
        queries.append(query)

    # assemble JDP-specific search parameters:
    jdp_params = {
        'f': 'project_id',     # search field likely containing IMG AP/SP ID
        'extra': 'project_id', # include this field in file metadata
    }

    # send the queries along and gather file info
    mapping = {}
    unmapped_ids = set([int(project_id) for project_id in project_ids if len(project_id) > 0])
    for query in queries:
        results = dts_client.search(database = 'jdp',
                                    query = query,
                                    specific = jdp_params)
        results = filter_results(results)
        for result in results:
            md     = result.to_dict()
            dts_id = md['id'] # DTS-sensible ID
            project_id_str = md['extra']['project_id'].replace('IMG_AP-', '') # IMG AP ID 
            project_id_str = project_id_str.replace('IMG_SP-', '') # IMG SP ID 
            project_id = int(project_id_str)
            file   = md['path']
            if ap_id not in mapping:
                mapping[ap_id] = {
                    'dts_id': dts_id,
                    'files': [file]
                }
                unmapped_aps.remove(ap_id)
            else:
                mapping[project_id]['files'].append(file)
    return mapping, list(unmapped_ids)
```

### Transferring data to a path within a specific Globus collection

This capability is only available to selected users, identified within the standalone authentication
system for the DTS.

```
def transfer_to_specific_globus_collection(dts_client, file_ids: []str, globus_collection: str, path: str) -> uuid.UUID:
    return dts_client.transfer(orcid = orcid,
                               file_ids = file_ids,
                               source = 'jdp',
                               destination = f'globus:{globus_collection}:{path}:globus')
```

Many more examples are available from Jeff by request (they need to be sanitized first, though).
