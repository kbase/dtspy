"""dts: a Python client for the Data Transfer Service.

The [Data Transfer System (DTS)](https://kbase.github.io/dts/) offers a federated
search capability for participating organizations in the DOE Biological and
Environmental Research program, and allows the transfer of related data and
metadata between these organizations.

DTS API documentation is available [here](https://lb-dts.staging.kbase.us/docs#/).

"""

from .database import Database
from .transfer_status import TransferStatus
from .client import Client
__all__ = ['Client', 'Database', 'TransferStatus']

