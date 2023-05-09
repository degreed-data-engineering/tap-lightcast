"""Stream class for tap-lightcast."""

import logging
import requests
from typing import Dict, Optional, Any
from singer_sdk import typing as th
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

    client_id = self.config["client_id"]
    client_secret = self.config["client_secret"]
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "emsi_open",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.request("POST", url=auth_endpoint, data=data, headers=headers)
    access_token = response.json()["access_token"]
    headers = {"Authorization": "Bearer {}".format(access_token)}
    url = url_base + "/meta"
    response = requests.request("GET", url=url, headers=headers)
    latestVersion = response.json()["data"]["latestVersion"]

    name = "skills_list"  # Stream name
    path = "/versions/{latestVersion}/skills".format(
        latestVersion=latestVersion
    )  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {"fields": "id"}
        if "limit" in self.config:
            params.update({"limit": self.config["limit"]})
        # if context["stopTap"] == True:
        #     params.update({"q": ""})
        return params

    schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"latestVersion": self.latestVersion, "id": record["id"]}


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
