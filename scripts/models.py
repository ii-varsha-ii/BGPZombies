from enum import Enum

from pydantic import BaseModel


class Types(Enum):
    ORIGIN = 1
    AS_PATH = 2
    NEXT_HOP = 3
    MULTI_EXIT_DISC = 4
    AGGREGATOR = 7
    COMMUNITY = 8


class BgpUpdateMessage(BaseModel):
    time: int
    from_as: str
    from_ip: str
    as_path: list[str] = []
    announce: str = None
    withdraw: str = None


class MRT_TYPES(Enum):
    TABLE_DUMP_V2 = 13
    BGP4MP = 16


class BGP4MP_SUBTYPES(Enum):
    BGP4MP_STATE_CHANGE = 0
    BGP4MP_MESSAGE = 1
    BGP4MP_MESSAGE_AS4 = 4
    BGP4MP_STATE_CHANGE_AS4 = 5
    BGP4MP_MESSAGE_LOCAL = 6
    BGP4MP_MESSAGE_AS4_LOCAL = 7


class BGP4MP_MESSAGE_AS4_TYPES(Enum):
    UPDATE = 2
