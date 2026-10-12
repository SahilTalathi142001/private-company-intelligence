import time
from typing import Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .config import SEC_BASE_URL, SEC_USER_AGENT


class SECAPIClient:

    def __init__(
        self,
        user_agent: str = SEC_USER_AGENT,
        timeout: int = 30,
        min_request_interval: float = 0.2,
    ):
        if not user_agent:
            raise ValueError(
                "SEC_USER_AGENT is missing. "
                "Add it to your .env file."
            )

        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.last_request_time = 0.0

        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        })

    def _rate_limit(self):

        elapsed = time.time() - self.last_request_time

        if elapsed < self.min_request_interval:

            sleep_time = (
                self.min_request_interval - elapsed
            )

            time.sleep(sleep_time)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(
            multiplier=1,
            min=1,
            max=10,
        ),
        retry=retry_if_exception_type(
            requests.RequestException
        ),
        reraise=True,
    )
    def get(
        self,
        endpoint: str,
    ) -> Any:

        self._rate_limit()

        url = f"{SEC_BASE_URL}{endpoint}"

        response = self.session.get(
            url,
            timeout=self.timeout,
        )

        self.last_request_time = time.time()

        response.raise_for_status()

        return response.json()