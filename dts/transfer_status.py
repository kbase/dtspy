from dataclasses import dataclass
from typing import Optional

@dataclass(slots = True)
class TransferStatus(object):
    """`TransferStatus` - holds status information for a file transfer"""
    id:                    str
    status:                str
    message:               Optional[str]
    num_files:             int
    num_files_transferred: int
