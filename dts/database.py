from dataclasses import dataclass

@dataclass(slots = True)
class Database(object):
    """`Database` - A database storing files that can be selected and transferred.

This type holds human-readable information about databases available to DTS.
Objects of this type are returned by calls to the DTS API, so it is not
necessary to construct them directly.
"""
    id:   str
    name: str
    organization: str
    url: str
