import base64
from frictionless.resources import JsonResource
import requests
from requests.auth import AuthBase
import logging
import uuid
from . import (
    Database,
    TransferStatus,
)
from urllib.error import (
    HTTPError,
)

logger = logging.getLogger('dts')
api_version = 1

class KBaseAuth(AuthBase):
    """Attaches a KBase-sensible Authorization header to the given Request object."""
    def __init__(self, api_key):
        self.api_key = api_key

    def __call__(self, request):
        b64_token = base64.b64encode(bytes(self.api_key + '\n', 'utf-8'))
        token = b64_token.decode('utf-8')
        request.headers['Authorization'] = f'Bearer {token}'
        return request

class Client:
    """`Client`: A client for performing file transfers with the Data Transfer System"""
    def __init__(self,
                 api_key = None, 
                 server  = None,
                 port    = None):
        """`Client(server = None, port = None, api_key = None)` -> DTS client.

* If no `server` is given, you must call `connect` on the created client."""
        if server:
            self.connect(server = server, port = port, api_key = api_key)
        else:
            self.uri = None
            self.name = None
            self.version = None

    def connect(self,
                api_key = None,
                server = None,
                port = None):
        """`client.connect(api_key = None, server = None, port = None)`

* Connects the client to the given DTS `server` via the given `port` using the given
  (unencoded) `api_key`."""
        if not isinstance(api_key, str):
            raise TypeError('api_key must be an unencoded API key.')
        if not isinstance(server, str):
            raise TypeError('server must be a URI for a DTS server.')
        if port and not isinstance(port, int):
            raise TypeError('port must be an integer')
        self.auth = KBaseAuth(api_key)
        if port:
            server = f'{server}:{port}'

        # perform a root query and fill in some information
        response = requests.get(server, auth = self.auth)
        response.raise_for_status()

        result = response.json()
        self.uri = f'{server}/api/v{api_version}'
        self.name = result['name']
        self.version = result['version']

    def disconnect(self):
        """`client.disconnect() -> None

* disconnects the client from the server."""
        self.api_key = None
        self.uri = None
        self.name = None
        self.version = None

    def databases(self):
        """`client.databases()` -> `list` of `Database` objects

* Returns all databases available to the service, or `None` if an error occurs."""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.get(self.uri + '/databases', auth = self.auth)
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        results = response.json()
        return [Database(id = r['id'],
                         name = r['name'],
                         organization = r['organization'],
                         url = r['url']) for r in results]

    def search(self,
               database = None,
               query = None,
               status = None,
               offset = 0,
               limit = None,
               specific = None,
    ):
        """
`client.search(database = None,
               query = None,
               status = None,
               offset = 0,
               limit = None,
               specific = None) -> `list` of `frictionless.DataResource` objects

* Performs a synchronous search of the database with the given name using the
  given query string.
Optional arguments:
    * query: a search string that is directly interpreted by the database
    * status: filters for files based on their status:
        * `"staged"` means "search only for files that are already in the source database staging area"
        * `"unstaged"` means "search only for files that are not staged"
    * offset: a 0-based index from which to start retrieving results (default: 0)
    * limit: if given, the maximum number of results to retrieve
    * specific: a dictionary mapping database-specific search parameters to their values
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if query:
            if not isinstance(query, str):
                # we also accept numeric values
                if isinstance(query, int) or isinstance(query, float):
                    query = str(query)
                else:
                    raise RuntimeError('search: query must be a string or a number.')
        else:
            raise RuntimeError('search: missing query.')
        if not isinstance(database, str):
            raise TypeError('search: database must be a string.')
        params = {
            'database': database,
            'query':    query,
        }
        if status:
            if status not in ['staged', 'unstaged']:
                raise TypeError(f'search: invalid status: {status}.')
            params['status'] = status
        if offset:
            if not str(offset).isdigit():
                raise TypeError('search: offset must be numeric')
            if int(offset) < 0:
                raise ValueError(f'search: offset must be non-negative')
            params['offset'] = int(offset)
        if limit:
            if not str(limit).isdigit():
                raise TypeError('search: limit must be numeric')
            if int(limit) < 1:
                raise ValueError(f'search: limit must be greater than 1')
            params['limit'] = int(limit)
        if specific:
            if not isinstance(specific, dict):
                raise TypeError('search: specific must be a dict.')
            params['specific'] = specific
        try:
            response = requests.post(url=f'{self.uri}/files',
                                     json=params,
                                     auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        return [JsonResource(r) for r in response.json()['resources']]

    def fetch_metadata(self,
               database = None,
               ids = None,
               offset = 0,
               limit = None,
    ):
        """
`client.fetch_metadata(database = None,
               ids = None,
               offset = 0,
               limit = None) -> `list` of `frictionless.DataResource` objects

* Fetches metadata for the files with the specified IDs within the specified
  database.
Optional arguments:
    * offset: a 0-based index from which to start retrieving results (default: 0)
    * limit: if given, the maximum number of results to retrieve
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if not isinstance(ids, list) or len(ids) == 0:
            raise RuntimeError('search: missing or invalid file IDs.')
        if not isinstance(database, str):
            raise TypeError('search: database must be a string.')
        params = {
            'database': database,
            'ids':    ','.join(ids),
        }
        if offset:
            if not str(offset).isdigit():
                raise TypeError('search: offset must be numeric')
            if int(offset) < 0:
                raise ValueError(f'search: offset must be non-negative')
            params['offset'] = int(offset)
        if limit:
            if not str(limit).isdigit():
                raise TypeError('search: limit must be numeric')
            if int(limit) < 1:
                raise ValueError(f'search: limit must be greater than 1')
            params['limit'] = int(limit)
        try:
            response = requests.get(url=f'{self.uri}/files/by-id',
                                    params=params,
                                    auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        return [JsonResource(r) for r in response.json()['resources']]

    def fetch_metadata(self,
               database = None,
               ids = None,
               offset = 0,
               limit = None,
    ):
        """
`client.fetch_metadata(database = None,
               ids = None,
               offset = 0,
               limit = None) -> `list` of `frictionless.DataResource` objects

* Fetches metadata for the files with the specified IDs within the specified
  database.
Optional arguments:
    * offset: a 0-based index from which to start retrieving results (default: 0)
    * limit: if given, the maximum number of results to retrieve
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if type(ids) != list or len(ids) == 0:
            raise RuntimeError('search: missing or invalid file IDs.')
        if type(database) != str:
            raise TypeError('search: database must be a string.')
        if type(offset) != int or offset < 0:
            raise TypeError(f'search: invalid offset: {offset}.')
        if limit:
            if type(limit) != int:
                raise TypeError('search: limit must be an int.')
            elif limit < 1:
                raise TypeError(f'search: invalid number of retrieved results: {N}')
        try:
            params = {
                'database': database,
                'ids':    ','.join(ids),
            }
            for name in ['offset', 'limit']:
                val = eval(name)
                if val:
                    params[name] = val
            response = requests.get(url=f'{self.uri}/files/by-id',
                                    params=params,
                                    auth=self.auth)
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            return None
        except requests.exceptions.HTTPError as err:
            logger.error(f'HTTP error occurred: {err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        else:
            return [JsonResource(r) for r in response.json()['resources']]

    def transfer(self,
                 file_ids = None,
                 source = None,
                 destination = None,
                 description = None,
                 timeout = None):
        """
`client.transfer(file_ids = None,
                 source = None,
                 destination = None,
                 description = None,
                 timeout = None) -> UUID

* Submits a request to transfer files from a source to a destination database. the
  files in the source database are identified by a list of string file_ids.
Optional arguments:
    * description: a string containing Markdown text describing the transfer
      (helpful for providing instructions to process the payload at its destination)
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if not isinstance(source, str):
            raise TypeError('transfer: source database name must be a string.')
        if not isinstance(destination, str):
            raise TypeError('transfer: destination database name must be a string.')
        if not isinstance(file_ids, list):
            raise TypeError('transfer: file_ids must be a list of string file IDs.')
        if timeout and not isinstance(timeout, int) and not isinstance(timeout, float):
            raise TypeError('transfer: timeout must be a number of seconds.')
        try:
            response = requests.post(url=f'{self.uri}/transfers',
                                     json={
                                         'source':      source,
                                         'destination': destination,
                                         'description': description,
                                         'file_ids':    file_ids,
                                     },
                                     auth=self.auth,
                                     timeout=timeout)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        return uuid.UUID(response.json()["id"])

    def transfer_status(self, id):
        """`client.transfer_status(id)` -> TransferStatus

* Returns status information for the transfer with the given identifier.
  Possible statuses are:
    * `'staging'`: the files requested for transfer are being copied to the staging
                   area for the source database job
    * `'active'`: the files are being transferred from the source database to the 
                  destination database
    * `'finalizing'`: the files have been transferred and a manifest is being written
    * `'inactive'`: the file transfer has been suspended
    * `'failed'`: the file transfer could not be completed because of a failure`
    * `'unknown'`: the status of the given transfer is unknown
* If an error is encountered, returns `None`."""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.get(url=f'{self.uri}/transfers/{id}',
                                    auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        results = response.json()
        return TransferStatus(
            id                    = results.get('id'),
            status                = results.get('status'),
            message               = results.get('message'),
            num_files             = results.get('num_files'),
            num_files_transferred = results.get('num_files_transferred'),
        )

    def cancel_transfer(self, id):
        """
`client.cancel_transfer(id) -> None

* Deletes a file transfer, canceling
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.delete(url=f'{self.uri}/transfers/{id}',
                                       auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return None
        return None

    def __repr__(self):
        if self.uri:
            return f"""
dts.Client(uri     = {self.uri},
           name    = {self.name},
           version = {self.version} """
        else:
            return "dts.Client(disconnected)"
