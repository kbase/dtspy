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
from typing import Any

logger = logging.getLogger('dts')
api_version = 1

class KBaseAuth(AuthBase):
    """Attaches a KBase-sensible Authorization header to the given Request object."""
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        b64_token = base64.b64encode(bytes(self.api_key + '\n', 'utf-8'))
        token = b64_token.decode('utf-8')
        request.headers['Authorization'] = f'Bearer {token}'
        return request

class Client:
    """`dts.Client`: A client for performing file transfers with the Data Transfer System (DTS).

This type exposes the [DTS API](https://lb-dts.staging.kbase.us/docs#/) for use
in Python programs.
"""
    def __init__(self,
                 api_key: str | None = None, 
                 server: str | None = None,
                 port: int | None = None):
        """Creates a DTS client that handles search and transfer requests via
a connected server.

If no server is specified, you must call `connect` on the created client.

Args:
    api_key: An unencoded KBase developer token.
    server: The DTS server that handles the client's API requests.
    port: The port to which the client connects with the server.

Returns:
    a `dts.Client` instance.

Raises:
    TypeError: an argument of improper type was specified.
"""
        if server:
            self.connect(server = server, port = port, api_key = api_key)
        else:
            self.uri = None
            self.name = None
            self.version = None

    def connect(self,
                api_key: str | None = None,
                server: str | None = None,
                port: int | None = None) -> None:
        """Connects the client to the given DTS server via the given port using the given
(unencoded) KBase developer token.

Args:
    api_key: An unencoded KBase developer token.
    server: The DTS server that handles the client's API requests.
    port: The port to which the client connects with the server.

Raises:
    TypeError: an argument of improper type was specified.
"""
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

    def disconnect(self) -> None:
        """Disconnects the client from the server.
"""
        self.api_key = None
        self.uri = None
        self.name = None
        self.version = None

    def databases(self) -> list[Database]:
        """Returns all databases available to the service.

Server-side errors are captured and logged.

Returns:
    A list of Database objects containing information about available databases.
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.get(self.uri + '/databases', auth = self.auth)
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            return []
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return []
        results = response.json()
        return [Database(id = r['id'],
                         name = r['name'],
                         organization = r['organization'],
                         url = r['url']) for r in results]

    def search(self,
               database: str,
               query: str | int | float,
               status: str | None,
               offset: int = 0,
               limit: int | None = None,
               specific: dict[str, Any] | None = None,
    ) -> list[JsonResource]:
        """Performs a synchronous search of the database with the given name using the given query string.

This method searches the indicated database for files that can be transferred.

Args:
    database: A string containing the name of the database to search.
    query: A search string that is directly interpreted by the database.
    status: An optional string (`"staged"` or `"unstaged"`) indicating whether files are filtered based on their status.
    offset: An optional 0-based pagination index indicating the first retrieved result (default: 0).
    limit: An optional pagination parameter indicating the maximum number of results to retrieve.
    specific: An optional dictionary mapping database-specific search parameters to their values.

Returns:
    A list of [frictionless DataResources](https://specs.frictionlessdata.io/data-resource/) containing metadata for files matching the query.

Raises:
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if not isinstance(query, str):
            # we also accept numeric values
            if isinstance(query, int) or isinstance(query, float):
                query = str(query)
            else:
                raise TypeError('search: query must be a string or a number.')
        else:
            raise RuntimeError('search: missing query.')
        if not isinstance(database, str):
            raise TypeError('search: database must be a string.')
        params: dict[str, Any] = {
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
            return []
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return []
        return [JsonResource(r) for r in response.json()['resources']]

    def fetch_metadata(self,
                       database: str,
                       ids: list[str],
                       offset: int = 0,
                       limit: int | None = None) -> list[JsonResource]:
        """Fetches metadata for the files with the specified IDs within the specified database.

Server-side errors are intercepted and logged.

Args:
    database: A string containing the name of the database to search.
    ids: A list containing file identifiers for which metadata is retrieved.
    offset: An optional 0-based pagination index from which to start retrieving results (default: 0).
    limit: An optional pagination parameter indicating the maximum number of results to retrieve.

Returns:
    A list of [frictionless DataResources](https://specs.frictionlessdata.io/data-resource/) containing metadata for files with the requested IDs.

Raises:
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        if not isinstance(ids, list) or len(ids) == 0:
            raise RuntimeError('search: missing or invalid file IDs.')
        if not isinstance(database, str):
            raise TypeError('search: database must be a string.')
        params: dict[str, Any] = {
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
            return []
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            return []
        return [JsonResource(r) for r in response.json()['resources']]

    def transfer(self,
                 file_ids: list[str],
                 source: str,
                 destination: str,
                 description: str | None = None,
                 instructions: dict[str, Any] | None = None,
                 timeout: int | None = None) -> uuid.UUID | None:
        """Submits a request to transfer files from a source to a destination database.

Server-side errors are intercepted and logged.

Args:
    file_ids: A list of identifiers for files to be transferred.
    source: The name of the database from which files are transferred.
    destination: The name of the database to which files are transferred.
    description: An optional string containing human-readable Markdown text describing the transfer.
    instructions: An optional dict representing a JSON object containing instructions for processing the payload at its destination.
    timeout: An optional integer indicating the number of seconds to wait for a response from the server.

Returns:
    A UUID uniquely identifying the file transfer that can be used to check its status, or None if a server-side error is encountered.

Raises:
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
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
        if description and not isinstance(description, str):
            raise TypeError('transfer: description must be a string containing Markdown.')
        if instructions and not isinstance(instructions, dict):
            raise TypeError('transfer: instructions must be a dict representing a JSON object containing machine-readable instructions for processing the payload at its destination.')
        json_obj = {
            'source':      source,
            'destination': destination,
            'file_ids':    file_ids,
        }
        if description:
            json_obj['description'] = description
        if instructions:
            json_obj['instructions'] = instructions
        try:
            response = requests.post(url=f'{self.uri}/transfers',
                                     json=json_obj,
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

    def transfer_status(self, id: uuid.UUID) -> TransferStatus | None:
        """Returns status information for the transfer with the given identifier.


Server-side errors are intercepted and logged. Possible transfer statuses are:

* `'staging'`: The files requested for transfer are being copied to the staging
               area for the source database job.
* `'active'`: The files are being transferred from the source database to the 
              destination database.
* `'finalizing'`: The files have been transferred and a manifest is being written.
* `'inactive'`: The file transfer has been suspended.
* `'failed'`: The file transfer could not be completed because of a failure.
* `'unknown'`: The status of the given transfer is unknown.

Arguments:
    id: A UUID that uniquely identifies the transfer operation for which the status is requested.

Returns:
    A `TransferStatus` object whose contents indicate the status of the transfer, or None if a server-side error occurs.

Raises:
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.get(url=f'{self.uri}/transfers/{id}',
                                    auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {err}')
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

    def cancel_transfer(self, id: uuid.UUID):
        """Cancels a file transfer with the requested UUID.

Status information for the cancelled transfer is retained for a time so its
cancellation can be seen.

Args:
    id: A UUID that uniquely identifies the transfer operation to be cancelled.

Raises:
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
"""
        if not self.uri:
            raise RuntimeError('dts.Client: not connected.')
        try:
            response = requests.delete(url=f'{self.uri}/transfers/{id}',
                                       auth=self.auth)
            response.raise_for_status()
        except (HTTPError, requests.exceptions.HTTPError) as err:
            logger.error(f'HTTP error occurred: {err}')
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
