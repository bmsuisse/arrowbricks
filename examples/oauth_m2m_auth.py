"""OAuth machine-to-machine auth (client_id + client_secret -> bearer token)
as a `token_provider` -- the same client-credentials flow
`databricks-sql-connector`'s `auth_type="databricks-oauth"` uses for service
accounts, implemented here with stdlib `urllib.request` only (arrowbricks
has zero required dependencies; unlike `examples/azure_auth.py`'s
azure-identity-based approach, this needs nothing extra installed).

Create a service principal + OAuth secret first: Account Console ->
User management -> Service principals, or `databricks account
service-principal-secrets create`. Needs `all-apis` scope, granted by
default to a workspace service principal.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_CLIENT_ID=... \\
    DATABRICKS_CLIENT_SECRET=... \\
    python examples/oauth_m2m_auth.py
"""

import asyncio
import base64
import json
import os
import time
import urllib.parse
import urllib.request

from arrowbricks import connect

# Refresh well before actual expiry, not just-in-time -- a long-running
# streamed query calls the provider on every request, and re-deriving a
# fresh token per call (rather than serving a cached one) would be
# needlessly slow. Same margin as examples/azure_auth.py.
_TOKEN_REFRESH_MARGIN_S = 300


class OAuthM2MTokenProvider:
    """Fetches and caches a bearer token via the OAuth client_credentials
    grant -- arrowbricks calls `token_provider` fresh on every request with
    no caching of its own, so the caching has to live here. The HTTP call is
    blocking, so it runs via asyncio.to_thread when called from the event
    loop (same reasoning as examples/azure_auth.py's AzureTokenProvider)."""

    def __init__(self, host: str, client_id: str, client_secret: str) -> None:
        self._token_url = f"https://{host}/oidc/v1/token"
        self._client_id = client_id
        self._client_secret = client_secret
        self._cached: tuple[str, float] | None = None  # (token, expires_at)

    def _get_or_refresh(self) -> str:
        if self._cached is None or self._cached[1] - _TOKEN_REFRESH_MARGIN_S <= time.monotonic():
            body = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode()
            basic_auth = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
            request = urllib.request.Request(  # noqa: S310 -- fixed https token endpoint, not user input
                self._token_url,
                data=body,
                headers={
                    "Authorization": f"Basic {basic_auth}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                payload = json.loads(response.read())
            self._cached = (payload["access_token"], time.monotonic() + payload["expires_in"])
        return self._cached[0]

    async def __call__(self) -> str:
        return await asyncio.to_thread(self._get_or_refresh)


async def main() -> None:
    token_provider = OAuthM2MTokenProvider(
        host=os.environ["DATABRICKS_HOST"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    async with connect(
        os.environ["DATABRICKS_HOST"],
        os.environ["DATABRICKS_WAREHOUSE_ID"],
        token_provider=token_provider,
    ) as conn:
        cursor = conn.cursor()
        await cursor.execute("SELECT 1 AS n")
        print(await cursor.fetchall())


if __name__ == "__main__":
    asyncio.run(main())
