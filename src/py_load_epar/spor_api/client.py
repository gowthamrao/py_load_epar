import logging
from typing import Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from py_load_epar.config import SporApiSettings
from py_load_epar.spor_api.models import SporOmsOrganisation, SporSmsSubstance

logger = logging.getLogger(__name__)


class SporApiClient:
    """
    A client for interacting with the SPOR API.

    Handles authentication, caching, and requests to SPOR endpoints.
    """

    def __init__(self, settings: SporApiSettings):
        self.settings = settings
        self._session = requests.Session()
        self._auth_token: Optional[str] = None
        self._org_cache: Dict[str, Optional[SporOmsOrganisation]] = {}
        self._substance_cache: Dict[str, Optional[SporSmsSubstance]] = {}

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _authenticate(self) -> None:
        """
        Authenticates with the SPOR API and stores the Bearer token.
        """
        if self._auth_token:
            return

        auth_url = f"{self.settings.base_url}/api/Account"
        logger.info(f"Authenticating with SPOR API at {auth_url}.")
        try:
            response = self._session.post(
                auth_url,
                json={
                    "tenancyName": self.settings.tenancy_name,
                    "username": self.settings.username,
                    "password": self.settings.password.get_secret_value(),
                },
                timeout=30,
            )
            response.raise_for_status()
            self._auth_token = response.json()["result"]["accessToken"]
            self._session.headers.update(
                {"Authorization": f"Bearer {self._auth_token}"}
            )
            logger.info("Successfully authenticated with SPOR API.")
        except requests.exceptions.RequestException as e:
            logger.error(f"SPOR API authentication failed: {e}")
            raise

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Makes an HTTP request with retry logic.
        Args:
            method: HTTP method (e.g., 'GET', 'POST').
            url: The URL for the request.
            **kwargs: Additional arguments for requests.request.
        Returns:
            The requests.Response object.
        """
        try:
            response = self._session.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request to {url} failed: {e}. Retrying...")
            raise

    def search_organisation(self, name: str) -> Optional[SporOmsOrganisation]:
        """
        Searches for an organisation by name in the SPOR OMS.
        Returns the first result if a high-confidence match is found.
        Caches results to avoid redundant API calls.
        """
        if name in self._org_cache:
            return self._org_cache[name]

        self._authenticate()
        search_url = f"{self.settings.base_url}/api/v1/spor/oms/organisations"
        params = {"name": name, "status": "Active", "pageSize": 2}

        try:
            response = self._make_request("GET", search_url, params=params)
            data = response.json()

            # High-confidence match: exactly one result found
            if len(data.get("items", [])) == 1:
                org_data = data["items"][0]
                organisation = SporOmsOrganisation.model_validate(org_data)
                self._org_cache[name] = organisation
                return organisation
            else:
                logger.debug(
                    f"Found {len(data.get('items', []))} results for '{name}'. "
                    "Not a high-confidence match."
                )
                self._org_cache[name] = None
                return None

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to search for organisation '{name}' after " f"retries: {e}"
            )
            return None

    def search_substance(self, name: str) -> Optional[SporSmsSubstance]:
        """
        Searches for a substance by name in the SPOR SMS.
        Returns the first result if a high-confidence match is found.
        Caches results to avoid redundant API calls.
        """
        if name in self._substance_cache:
            return self._substance_cache[name]

        self._authenticate()
        search_url = f"{self.settings.base_url}/api/v1/spor/sms/substances"
        params = {"name": name, "status": "Current", "pageSize": 2}

        try:
            response = self._make_request("GET", search_url, params=params)
            data = response.json()

            if len(data.get("items", [])) == 1:
                substance_data = data["items"][0]
                substance = SporSmsSubstance.model_validate(substance_data)
                self._substance_cache[name] = substance
                return substance
            else:
                logger.debug(
                    f"Found {len(data.get('items', []))} results for '{name}'. "
                    "Not a high-confidence match."
                )
                self._substance_cache[name] = None
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to search for substance '{name}' after retries: {e}")
            return None
