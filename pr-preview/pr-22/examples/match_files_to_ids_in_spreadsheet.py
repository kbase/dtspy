# This file extracts ITS AP IDs from a spreadsheet, finds files with those IDs,
# and writes out another spreadsheet with specific file information.

import csv
import dts
import os

def extract_columns(csv_file: str, column_names: list = []):
    with open(csv_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        indices = [-1 for _ in column_names]
        columns = [[] for _ in column_names]
        for row in reader:
            for i in range(len(column_names)):
                if indices[i] == -1: # header row
                    indices[i] = 0
                    while row[indices[i]] != column_names[i]:
                        indices[i] += 1
                else:
                    columns[i].append(row[indices[i]])
    return columns

# filter out unwanted files
def filter_results(resources: list):
   return [r for r in resources
           if (os.path.basename(r.path).startswith('Ga') and
               (r.path.endswith('_proteins.faa') and
                not (r.path.endswith('genemark_proteins.faa') or
                     r.path.endswith('prodigal_proteins.faa')))) or
           r.path.endswith('.assembled.faa') or
           (os.path.basename(r.path).startswith('Ga') and
            (r.path.endswith('_cds.gff') or r.path.endswith('_contigs.fna'))) or
           r.path.endswith('.assembled.fna') or r.path.endswith('.assembled.gff')]

# returns true if the given list of files consists of
# * unpaired *contig.fna files and/or
# * *_cds.gff files paired with their *contig.fna counterparts
def files_are_paired(files: list):
    contigs, cdses = set(), set()
    for file in files:
        if file.startswith('Ga') and file.endswith('_contigs.fna'):
            contigs.add(file.replace('_contigs.fna', ''))
        elif file.endswith('.assembled.fna'):
            contigs.add(file.replace('.assembled.fna', ''))
        elif file.startswith('Ga') and file.endswith('_cds.gff'):
            cdses.add(file.replace('_cds.gff', ''))
        elif file.endswith('.assembled.gff'):
            cdses.add(file.replace('.assembled.gff', ''))
    for cds in cdses:
        if not cds in contigs:
            return False
    return True

def map_ids(dts_client: dts.Client, csv_file: str):
    # fetch ITS AP/SP IDs
    (its_ap_ids, taxon_oids) = extract_columns(csv_file = csv_file,
                                               column_names = ['ITS AP ID - 90 Complete', 'taxon_oid'])
    query = ''
    for its_id in its_ap_ids:
        if len(query) == 0:
            query += its_id
        elif len(query) + 1 + len(its_id) <= 1000:
            query += f' | {its_id}'

    # assemble some JDP-specific search parameters:
    jdp_params = {
        'f': 'project_id',     # search field likely containing IMG AP/SP ID
        'extra': 'project_id', # include this field in file metadata
    }

    # send the queries along and gather file info
    mapping  = {}
    results = dts_client.search(database = 'jdp',
                                query = query,
                                specific = jdp_params)
    results = filter_results(results)
    for i, result in enumerate(results):
        md     = result.to_dict()
        dts_id = md['id'] # DTS-sensible ID
        if md['extra']['project_id'].startswith('IMG_AP-'):
            ap_id  = int(md['extra']['project_id'].replace('IMG_AP-', '')) # IMG AP ID
            file   = md['path']
            if ap_id not in mapping:
                mapping[ap_id] = {
                    'taxon_oid': taxon_oids[i],
                    'dts_ids': [dts_id],
                    'files': [file]
                }
            else:
                mapping[ap_id]['dts_ids'].append(dts_id)
                mapping[ap_id]['files'].append(file)
        print('.', end='', flush=True)
    print('.')

    for ap_id in mapping.keys():
        if not files_are_paired(mapping[ap_id]['files']):
            del mapping[ap_id]
    return mapping

def write_spreadsheet(csv_file: str, mapping: dict):
    with open(csv_file, 'w', newline='') as csvfile:
        field_names = ['IMG AP ID', 'IMG taxon OID', 'DTS ID', 'file']
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        for ap_id, md in mapping.items():
            files = md['files']
            if files_are_paired(files):
                taxon_oid = md['taxon_oid']
                dts_ids = md['dts_ids']
                for i, file in enumerate(files):
                    writer.writerow({
                        'IMG AP ID': ap_id,
                        'IMG taxon OID': taxon_oid,
                        'DTS ID': dts_ids[i],
                        'file': file,
                    })

def main():
    # connect to the DTS
    token = os.getenv('DTS_KBASE_DEV_TOKEN')
    dts_client = dts.Client(api_key = token,
                            server = "https://lb-dts.staging.kbase.us")

    print("Mapping IDs from IMG.csv...")
    mapping = map_ids(dts_client, 'IMG.csv')

    print("Writing files_with_ids.csv...")
    write_spreadsheet('files_with_ids.csv', mapping)

if __name__ == '__main__':
    main()
