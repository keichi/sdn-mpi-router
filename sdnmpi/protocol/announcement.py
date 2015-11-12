from construct import Struct, Union, Enum, SLInt32

announcement_type = Enum(
    SLInt32("type"),
    LAUNCH=0,
    EXIT=1,
)

announcement = Struct(
    "announcement",
    announcement_type,
    Union(
        "args",
        SLInt32("rank"),
    ),
)

ANNOUNCEMENT_PACKET_LEN = announcement.sizeof()
