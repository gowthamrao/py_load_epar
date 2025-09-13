
import pytest
import requests_mock

from py_load_epar.config import SporApiSettings
from py_load_epar.spor_api.client import SporApiClient
from py_load_epar.spor_api.models import SporOmsOrganisation, SporSmsSubstance


@pytest.fixture
def spor_settings() -> SporApiSettings:
    """Provides a sample SporApiSettings object for tests."""
    return SporApiSettings(
        base_url="https://test.spor.api",
        tenancy_name="test-tenant",
        username="testuser",
        password="testpassword",
    )


def test_authenticate_success(spor_settings):
    """
    Test that the client successfully authenticates and stores the token.
    """
    client = SporApiClient(spor_settings)
    with requests_mock.Mocker() as m:
        m.post(
            f"{spor_settings.base_url}/api/Account",
            json={"result": {"accessToken": "fake-token"}},
        )
        client._authenticate()
        assert client._auth_token == "fake-token"
        assert (
            client._session.headers["Authorization"] == "Bearer fake-token"
        )


def test_authenticate_is_cached(spor_settings):
    """
    Test that the client authenticates only once and caches the token.
    """
    client = SporApiClient(spor_settings)
    with requests_mock.Mocker() as m:
        mock_post = m.post(
            f"{spor_settings.base_url}/api/Account",
            json={"result": {"accessToken": "fake-token"}},
        )
        # Call authenticate multiple times
        client._authenticate()
        client._authenticate()
        # The mock should have been called only once
        assert mock_post.call_count == 1


def test_search_organisation_success(spor_settings):
    """
    Test organisation search with a single, high-confidence result.
    """
    client = SporApiClient(spor_settings)
    org_name = "Test Pharma"
    api_response = {
        "items": [{"orgId": "ORG-123", "name": org_name}]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/oms/organisations", json=api_response)

        result = client.search_organisation(org_name)

        assert isinstance(result, SporOmsOrganisation)
        assert result.org_id == "ORG-123"
        # Check that the result is cached
        assert org_name in client._org_cache
        assert client._org_cache[org_name] is result


def test_search_organisation_no_match(spor_settings):
    """
    Test organisation search with zero results.
    """
    client = SporApiClient(spor_settings)
    org_name = "Unknown Pharma"
    api_response = {"items": []}

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/oms/organisations", json=api_response)

        result = client.search_organisation(org_name)

        assert result is None
        assert client._org_cache[org_name] is None


def test_search_organisation_ambiguous_match(spor_settings):
    """
    Test organisation search with multiple results (low-confidence).
    """
    client = SporApiClient(spor_settings)
    org_name = "Ambiguous Pharma"
    api_response = {
        "items": [
            {"orgId": "ORG-1", "name": "Ambiguous Pharma One"},
            {"orgId": "ORG-2", "name": "Ambiguous Pharma Two"},
        ]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/oms/organisations", json=api_response)

        result = client.search_organisation(org_name)

        assert result is None
        assert client._org_cache[org_name] is None


def test_search_organisation_is_cached(spor_settings):
    """
    Test that organisation search results are cached.
    """
    client = SporApiClient(spor_settings)
    org_name = "Test Pharma"
    api_response = {
        "items": [{"orgId": "ORG-123", "name": org_name}]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        mock_get = m.get(f"{spor_settings.base_url}/api/v1/spor/oms/organisations", json=api_response)

        # Search for the same name twice
        client.search_organisation(org_name)
        client.search_organisation(org_name)

        # The mock GET should have been called only once
        assert mock_get.call_count == 1


def test_search_organisation_retries_on_failure(spor_settings):
    """
    Test that the client retries the search request on transient server errors.
    """
    client = SporApiClient(spor_settings)
    org_name = "Retry Pharma"
    success_response = {
        "items": [{"orgId": "ORG-RETRY", "name": org_name}]
    }

    with requests_mock.Mocker() as m:
        # Mock authentication
        m.post(
            f"{spor_settings.base_url}/api/Account",
            json={"result": {"accessToken": "fake-token"}},
        )

        search_url = f"{spor_settings.base_url}/api/v1/spor/oms/organisations"
        # The mock will first respond with a 503 error, then with a 200 OK
        mock_get = m.get(
            search_url,
            [
                {"status_code": 503, "reason": "Service Unavailable"},
                {"status_code": 200, "json": success_response},
            ],
        )

        result = client.search_organisation(org_name)

        # Assert that the request was tried twice
        assert mock_get.call_count == 2
        assert isinstance(result, SporOmsOrganisation)
        assert result.org_id == "ORG-RETRY"


def test_authenticate_failure(spor_settings):
    """
    Test that authentication raises an exception on API error.
    """
    client = SporApiClient(spor_settings)
    with requests_mock.Mocker() as m:
        m.post(
            f"{spor_settings.base_url}/api/Account",
            status_code=500,
            reason="Internal Server Error",
        )
        with pytest.raises(Exception):
            client._authenticate()


def test_search_substance_success(spor_settings):
    """
    Test substance search with a single, high-confidence result.
    """
    client = SporApiClient(spor_settings)
    substance_name = "Test-substance"
    api_response = {
        "items": [{"smsId": "SUB-123", "name": substance_name}]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/sms/substances", json=api_response)

        result = client.search_substance(substance_name)

        assert isinstance(result, SporSmsSubstance)
        assert result.sms_id == "SUB-123"
        assert substance_name in client._substance_cache
        assert client._substance_cache[substance_name] is result


def test_search_substance_no_match(spor_settings):
    """
    Test substance search with zero results.
    """
    client = SporApiClient(spor_settings)
    substance_name = "Unknown Substance"
    api_response = {"items": []}

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/sms/substances", json=api_response)

        result = client.search_substance(substance_name)

        assert result is None
        assert client._substance_cache[substance_name] is None


def test_search_substance_ambiguous_match(spor_settings):
    """
    Test substance search with multiple results (low-confidence).
    """
    client = SporApiClient(spor_settings)
    substance_name = "Ambiguous Substance"
    api_response = {
        "items": [
            {"smsId": "SUB-1", "name": "Ambiguous Substance One"},
            {"smsId": "SUB-2", "name": "Ambiguous Substance Two"},
        ]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        m.get(f"{spor_settings.base_url}/api/v1/spor/sms/substances", json=api_response)

        result = client.search_substance(substance_name)

        assert result is None
        assert client._substance_cache[substance_name] is None


def test_search_substance_is_cached(spor_settings):
    """
    Test that substance search results are cached.
    """
    client = SporApiClient(spor_settings)
    substance_name = "Test-substance"
    api_response = {
        "items": [{"smsId": "SUB-123", "name": substance_name}]
    }

    with requests_mock.Mocker() as m:
        m.post(f"{spor_settings.base_url}/api/Account", json={"result": {"accessToken": "fake-token"}})
        mock_get = m.get(f"{spor_settings.base_url}/api/v1/spor/sms/substances", json=api_response)

        client.search_substance(substance_name)
        client.search_substance(substance_name)

        assert mock_get.call_count == 1


def test_search_substance_retries_on_failure(spor_settings):
    """
    Test that the client retries the search request on transient server errors.
    """
    client = SporApiClient(spor_settings)
    substance_name = "Retry Substance"
    success_response = {
        "items": [{"smsId": "SUB-RETRY", "name": substance_name}]
    }

    with requests_mock.Mocker() as m:
        m.post(
            f"{spor_settings.base_url}/api/Account",
            json={"result": {"accessToken": "fake-token"}},
        )

        search_url = f"{spor_settings.base_url}/api/v1/spor/sms/substances"
        mock_get = m.get(
            search_url,
            [
                {"status_code": 503, "reason": "Service Unavailable"},
                {"status_code": 200, "json": success_response},
            ],
        )

        result = client.search_substance(substance_name)

        assert mock_get.call_count == 2
        assert isinstance(result, SporSmsSubstance)
        assert result.sms_id == "SUB-RETRY"
