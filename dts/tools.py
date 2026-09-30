"""MCP tool functions for the DTS Python client."""

from __future__ import annotations

import time
from frictionless.resources import JsonResource
from typing import Any

# NOTE: This is just a rough sketch of how these tools can use the DTS Python client. 
# TODO: Configure the MCP server. See for example https://github.com/kbaseincubator/env-data-mcp/blob/main/src/env_data_mcp/server.py
# TODO: Figure out how to handle exceptions thrown by the client.

from ??? import mcp

# server instance
server = "https://dts.dev.berdl.kbase.us:31276"

@mcp.tool()
def list_databases(*, token: str) -> list[Database]:
    """List databases (data sources) available to DTS.

    Returns ``list[Database]`` containing information for all available databases.

    ### Args
    * token: a KBase user token used to authenticate for the DTS API.

    ### Raises
    * TypeError: an argument of improper type was specified.
    """
    client = Client(api_key = token, server = server)
    return client.databases()

@mcp.tool()
def search_database(*,
                    token: str,
                    orcid: str,
                    database: str,
                    query: str | int | float,
                    status: str | None = None,
                    offset: int = 0,
                    limit: int | None = None,
                    specific: dict[str, Any] | None = None) -> list[JsonResource]:
    """Searches the database with the given name using the given query string.

    Returns a list of [frictionless DataResources](https://specs.frictionlessdata.io/data-resource/)
    containing metadata for files matching the query.

    ### Args
    * database: A string containing the name of the database to search.
    * orcid: An ORCID for the user searching for files.
    * query: A search string that is directly interpreted by the database.
    * status: An optional string (`"staged"` or `"unstaged"`) indicating whether files are filtered based on their status.
    * offset: An optional 0-based pagination index indicating the first retrieved result (default: 0).
    * limit: An optional pagination parameter indicating the maximum number of results to retrieve.
    * specific: An optional dictionary mapping database-specific search parameters to their values.

    ### Raises
    * RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    * TypeError: Indicates that an argument passed to the client isn't of the proper type.
    * ValueError: Indicates that an argument passed to the client has an invalid value.
    """
    client = Client(api_key = token, server = server)
    return client.search(database = database, orcid = orcid, query = query, status = status,
                         offset = offset, limit = limit, specific = specific)

@mcp.tool()
def transfer_files(*, token: str, orcid: str, source: str, destination: str, file_ids: []str, timeout: int | None = None) -> uuid.UUID | None:
    """Submit a request to transfer files from a source to a destination database. Server-side
    errors are intercepted and logged.

    Returns a UUID that uniquely identifies the file transfer that can be used to check its status,
    or None if a server-side error is encountered.

    ### Args
    * orcid: An ORCID for the user requesting the transfer.
    * file_ids: A list of identifiers for files to be transferred.
    * source: The name of the database from which files are transferred.
    * destination: The name of the database to which files are transferred.
    * description: An optional string containing human-readable Markdown text describing the transfer.
    * instructions: An optional dict representing a JSON object containing instructions for processing the payload at its destination.
    * timeout: An optional integer indicating the number of seconds to wait for a response from the server.

    ### Raises
    RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    TypeError: Indicates that an argument passed to the client isn't of the proper type.
    ValueError: Indicates that an argument passed to the client has an invalid value.
    """
    client = Client(api_key = token, server = server)
    return client.transfer(orcid = orcid, source = source, destination = destination, timeout = timeout)

@mcp.tool()
def check_status(*, token: str, transfer_id: uuid.UUID) -> TransferStatus | None: 
    """Returns status information for the transfer with the given identifier. Server-side errors are
    intercepted and logged.

    Returns a `TransferStatus` object whose contents indicate the status of the transfer, or None
    if a server-side error occurs. Possible transfer status codes are:
    * staging: The files requested for transfer are being copied to the staging
                   area for the source database job.
    * active: The files are being transferred from the source database to the 
                  destination database.
    * finalizing: The files have been transferred and a manifest is being written.
    * inactive: The file transfer has been suspended.
    * failed: The file transfer could not be completed because of a failure.
    * unknown: The status of the given transfer is unknown.

    ### Args
    * id: A UUID that uniquely identifies the transfer operation for which the status is requested.

    ### Raises
    * RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    * TypeError: Indicates that an argument passed to the client isn't of the proper type.
    * ValueError: Indicates that an argument passed to the client has an invalid value.
    """
    client = Client(api_key = token, server = server)
    return client.transfer_status(transfer_id)

@mcp.tool()
def cancel_transfer(*, token: str, orcid: str, transfer_id: uuid.UUID) -> None: 
    """Cancels a file transfer with the requested UUID. Status information for the cancelled
    transfer is retained for a time so its cancellation can be seen.

    ### Args
    * id: A UUID that uniquely identifies the transfer operation to be cancelled.
    * orcid: The ORCID of the user that initiated the transfer.

    ### Raises:
    * RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    * TypeError: Indicates that an argument passed to the client isn't of the proper type.
    * ValueError: Indicates that an argument passed to the client has an invalid value.
"""
    client = Client(api_key = token, server = server)
    client.cancel_transfer(id = transfer_id, orcid = orcid)

@mcp.tool()
def fetch_metadata(*, token: str, orcid: str, database: str, file_ids: []str, offset: int = 0, limit: int | None = None) -> list[JsonResource]: 
    """Fetches metadata for the files with the specified IDs within the specified database.
Server-side errors are intercepted and logged.

    Returns a list of [frictionless DataResources](https://specs.frictionlessdata.io/data-resource/)o
    containing metadata for files with the requested IDs.

    ### Args
    * database: A string containing the name of the database to search.
    * orcid: An ORCID for the user requesting the metadata.
    * file_ids: A list containing file identifiers for which metadata is retrieved.
    * offset: An optional 0-based pagination index from which to start retrieving results (default: 0).
    * limit: An optional pagination parameter indicating the maximum number of results to retrieve.

    ### Raises
    * RuntimeError: Indicates an issue with the DTS client and its connection to the server.
    * TypeError: Indicates that an argument passed to the client isn't of the proper type.
    * ValueError: Indicates that an argument passed to the client has an invalid value.
    """
    client = Client(api_key = token, server = server)
    return client.fetch_metadata(database = database,
                                 orcid = orcid, 
                                 file_ids = file_ids,
                                 offset = offset,
                                 limit = limit)

