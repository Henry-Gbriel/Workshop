import requests
from typing import Optional

BASE_URL = "https://pokeapi.co/api/v2/"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


class PokeAPIService:
    def __init__(self):
        self.requests = 0

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict = {},
    ) -> Optional[requests.Response]:
        session = None
        response = None

        try:
            self.requests += 1
            print(f"Making request #{self.requests} to {endpoint} with params {params}")
            response = requests.request(
                method=method,
                url=f"{BASE_URL}{endpoint}",
                headers=HEADERS,
                params=params,
            )

        except Exception as e:
            raise e
        finally:
            if session:
                session.close()

        return response