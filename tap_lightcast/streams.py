"""Stream class for tap-lightcast."""

import logging
import requests
from typing import Dict, Optional, Any
from singer_sdk import typing as th
from singer_sdk import Tap
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, OAuthAuthenticator

logging.basicConfig(level=logging.INFO)

url_base = f"https://emsiservices.com/skills"
auth_endpoint = f"https://auth.emsicloud.com/connect/token"


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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        logging.info("Parse Lightcast latest version")
        # Create new token
        data = {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
            "scope": "emsi_open",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.request(
            "POST", url=auth_endpoint, data=data, headers=headers
        )
        access_token = response.json()["access_token"]
        # Parse latestVersion
        headers = {"Authorization": "Bearer {}".format(access_token)}
        url = url_base + "/meta"
        response = requests.request("GET", url=url, headers=headers)
        self.latestVersion = response.json()["data"]["latestVersion"]
        # if "replication_key_value" in self.stream_state:
        #     logging.warn(
        #         "################## replication_key_value already in stream_state"
        #     )
        #     logging.warn(str(self.stream_state))
        #     self.replication_key = self.stream_state["replication_key"]
        #     self.replication_key_value = self.stream_state["replication_key_value"]
        # else:
        #     logging.warn("################## no replication_key_value in stream_state")
        #     logging.warn(str(self.stream_state))
        #     self.stream_state["replication_key"] = "latestVersion"
        #     self.stream_state["replication_key_value"] = self.latestVersion
        #     self.replication_key = "latestVersion"
        #     self.replication_key_value = ""
        #     logging.warn(self.stream_state)

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return url_base

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return LightcastOAuthAuthenticator(
            stream=self,
            auth_endpoint=auth_endpoint,
        )


class SkillsList(TapLightcastStream):
    # def __init__(self, tap: Tap):
    #     super().__init__(tap)
    #     self.logger = logging.getLogger(__name__)

    name = "skills_list"  # Stream name
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path
    path = ""
    replication_key = "latestVersion"

    schema = th.PropertiesList(
        th.Property("id", th.StringType), th.Property("latestVersion", th.StringType)
    ).to_dict()

    
    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        logging.warn("##########" + str(self.stream_state))
        http_method = self.rest_method
        url: str = "{url_base}/versions/{latestVersion}/skills".format(
            url_base=url_base, latestVersion=self.latestVersion
        )
        params = {"fields": "id"}
        if "limit" in self.config:
            params.update({"limit": self.config["limit"]})
        # if "replication_key_value" in self.stream_state:
        #     if self.stream_state["replication_key_value"] == self.latestVersion:
        #         params.update({"q": ""})
        headers = self.http_headers
        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
        )

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["latestVersion"] = self.latestVersion
        return row

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"latestVersion": self.latestVersion, "id": record["id"]}


class SkillsDetails(TapLightcastStream):
    parent_stream_type = SkillsList
    name = "skills_details"  # Stream name
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path
    path = "/versions/{latestVersion}/skills/{id}"  # API endpoint after base_url

    schema = th.PropertiesList(
        th.Property("latestVersion", th.StringType),
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
