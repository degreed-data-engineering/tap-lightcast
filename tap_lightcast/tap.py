"""lightcast tap class."""
from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_lightcast.streams import (
    SkillsList,
    SkillsDetails,
)

PLUGIN_NAME = "tap-lightcast"

STREAM_TYPES = [
    SkillsList,
    SkillsDetails,
]


class TapLightcast(Tap):
    """lightcast tap class."""

    name = "tap-lightcast"
    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True, description="Client ID"),
        th.Property(
            "client_secret", th.StringType, required=True, description="Client Secret"
        ),
        th.Property(
            "limit",
            th.IntegerType,
            required=False,
            description="Used for debugging purposes. It limits the number of IDs to select.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return streams


# CLI Execution:
cli = TapLightcast.cli
