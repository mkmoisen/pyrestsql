def get(test_client, url, headers=None, expected_status_code=200, **query_params):
    headers = headers or {}

    r = test_client.get(
        url,
        headers=headers,
        query_string=query_params
    )

    assert r.status_code == expected_status_code, r.json
    return r.json


def post(test_client, url, headers=None, expected_status_code=201, **json):
    headers = headers or {}

    r = test_client.post(
        url,
        json=json,
        headers=headers
    )

    assert r.status_code == expected_status_code, r.json
    return r.json


def delete(test_client, url, headers=None, expected_status_code=200):
    headers = headers or {}

    r = test_client.delete(
        url,
        headers=headers
    )

    assert r.status_code == expected_status_code, r.json
    return r.json


def patch(test_client, url, headers=None, expected_status_code=200, **json):
    headers = headers or {}

    r = test_client.patch(
        url,
        json=json,
        headers=headers
    )

    assert r.status_code == expected_status_code, r.json
    return r.json


class RestTokenTester:
    def __init__(self, test_client, url_prefix):
        self.test_client = test_client
        if url_prefix[-1] == '/':
            url_prefix = url_prefix[:-1]
        self.url_prefix = url_prefix

    def _make_authorization_headers(self, token):
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'

        return headers

    def get(self, token, pk, expected_status_code=200):
        headers = self._make_authorization_headers(token)
        return get(self.test_client, f'{self.url_prefix}/{pk}/', headers, expected_status_code)

    def get_many(self, token, expected_status_code=200, **query_params):
        headers = self._make_authorization_headers(token)
        return get(self.test_client, self.url_prefix, headers, expected_status_code, **query_params)

    def post(self, token, expected_status_code=201, **json):
        headers = self._make_authorization_headers(token)
        return post(self.test_client, self.url_prefix, headers, expected_status_code, **json)

    def patch(self, token, pk, expected_status_code=200, **json):
        headers = self._make_authorization_headers(token)
        return patch(self.test_client, f'{self.url_prefix}/{pk}/', headers, expected_status_code, **json)

    def delete(self, token, pk, expected_status_code=200):
        headers = self._make_authorization_headers(token)
        return delete(self.test_client, f'{self.url_prefix}/{pk}/', headers, expected_status_code)
