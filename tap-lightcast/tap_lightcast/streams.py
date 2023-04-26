"""Stream class for tap-lightcast."""

import logging
from typing import cast, Dict, Optional, Any, Iterable
from pathlib import Path
from urllib.parse import parse_qsl
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, OAuthAuthenticator
from singer_sdk.pagination import BaseHATEOASPaginator

logging.basicConfig(level=logging.INFO)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


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


# class CustomHATEOASPaginator(BaseHATEOASPaginator):
#     """
#     Link 1: https://sdk.meltano.com/en/latest/classes/singer_sdk.pagination.BaseHATEOASPaginator.html
#     Link 2: https://sdk.meltano.com/en/latest/guides/pagination-classes.html#example-hateoas-pagination-a-k-a-next-links
#     """

# def get_next_url(self, response):
#     data = response.json()
#     _links = data.get("_links")
#     if "next" in _links:
#         next = _links["next"]["href"]
#     else:
#         next = None
#     return next


class TapLightcastStream(RESTStream):
    """Lightcast stream class."""

    url_base = "https://emsiservices.com/skills/versions"

    # def get_new_paginator(self):
    #     return CustomHATEOASPaginator()

    # def get_url_params(
    #     self, context: Optional[dict], next_page_token: Optional[Any]
    # ) -> Dict[str, Any]:
    #     """Return a dictionary of values to be used in URL parameterization."""
    #     params = {}
    #     params.update({"psize": self.config["page_size"]})
    #     params.update({"activeOnly": self.config["active_only"]})
    #     params.update({"language": self.config["language"]})
    #     if next_page_token:
    #         params.update(parse_qsl(next_page_token.query))
    #     return params

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return LightcastOAuthAuthenticator(
            stream=self,
            auth_endpoint=f"https://auth.emsicloud.com/connect/token",
        )


class Skills(TapLightcastStream):
    name = "skills"  # Stream name
    # path = "/latest"  # API endpoint after base_url
    # primary_keys = ["id"]
    # records_jsonpath = "$.items[*]"  # https://jsonpath.com Use requests response json to identify the json path

    # schema = th.PropertiesList(
    #     th.Property("author", th.StringType),
    #     th.Property("channels", th.StringType),
    #     th.Property("coverUrl", th.StringType),
    #     th.Property("duration", th.StringType),
    #     th.Property("id", th.IntegerType),
    #     th.Property("introHtml", th.StringType),
    #     th.Property("source", th.ObjectType(th.Property("pages", th.IntegerType))),
    #     th.Property("title", th.StringType),
    #     th.Property("type", th.StringType),
    #     th.Property("url", th.StringType),
    # ).to_dict()
