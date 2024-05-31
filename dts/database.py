from dataclasses import dataclass

@dataclass(slots = True)
class Database(object):
    """`Database` - A database storing files that can be selected and transferred"""
    id:   str
    name: str
    organization: str
    url: str
