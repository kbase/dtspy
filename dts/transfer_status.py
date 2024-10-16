from dataclasses import dataclass
from typing import Optional

@dataclass(slots = True)
class TransferStatus:
    """`TransferStatus` status information for a file transfer.

This type holds information pertaining to the transfer of a payload initiated
via the DTS. Objects of this type are returned by calls to the DTS API, so it
is not necessary to create them directly.
"""
    id:                    str
    status:                str
    message:               Optional[str]
    num_files:             int
    num_files_transferred: int
