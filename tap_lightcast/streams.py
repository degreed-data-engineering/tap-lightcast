"""Stream class for tap-lightcast."""

import logging
import requests
from typing import Dict, Optional, Any
from singer_sdk import typing as th
from singer_sdk import Tap
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, OAuthAuthenticator

logging.basicConfig(level=logging.INFO)


class LightcastOAuthAuthenticator(OAuthAuthenticator):
    """Authenticate with client credentials"""

    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
            "scope": "emsi_open",
        }


class TapLightcastStream(RESTStream):
    """Generic Lightcast stream class."""

    url_base = "https://emsiservices.com/skills"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return LightcastOAuthAuthenticator(
            stream=self,
            auth_endpoint=f"https://auth.emsicloud.com/connect/token",
        )


class SkillsLatestVersion(TapLightcastStream):
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.replication_key_value = ""

    name = "skills_latest_version"  # Stream name
    path = "/meta"  # API endpoint after base_url
    primary_keys = ["latestVersion"]
    records_jsonpath = "$.data"  # https://jsonpath.com Use requests response json to identify the json path

    schema = th.PropertiesList(th.Property("latestVersion", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""

        def getLatestVersion(self, record: dict, context: Optional[dict]):
            latestVersion = record["latestVersion"]
            replication_key_value = str(
                self.get_starting_replication_key_value(context)
            )
            if latestVersion != replication_key_value:
                logging.info("latestVersion: " + str(latestVersion))
                logging.info("replication_key_value: " + str(replication_key_value))
                self.replication_key_value = latestVersion
                return latestVersion
            else:
                return None

        latestVersion = getLatestVersion(self, record, context)
        return {"latestVersion": latestVersion}


class SkillsList(TapLightcastStream):
    name = "skills_list"  # Stream name
    parent_stream_type = SkillsLatestVersion
    path = "/versions/{latestVersion}/skills"  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {"fields": "id"}
        if "limit" in self.config:
            params.update({"limit": self.config["limit"]})
        return params

    schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"latestVersion": context["latestVersion"], "id": record["id"]}


class SkillsDetails(TapLightcastStream):
    name = "skills_details"  # Stream name
    parent_stream_type = SkillsList
    path = "/versions/{latestVersion}/skills/{id}"  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.ObjectType(th.Property("id", th.StringType))),
        th.Property("type", th.ObjectType(th.Property("name", th.StringType))),
        th.Property("category", th.ObjectType(th.Property("id", th.IntegerType))),
        th.Property("category", th.ObjectType(th.Property("name", th.StringType))),
        th.Property("subcategory", th.ObjectType(th.Property("id", th.IntegerType))),
        th.Property("subcategory", th.ObjectType(th.Property("name", th.StringType))),
        th.Property("isLanguage", th.BooleanType),
        th.Property("isSoftware", th.BooleanType),
        th.Property("description", th.StringType),
        th.Property("descriptionSource", th.StringType),
    ).to_dict()
