import unittest


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if cls.__name__ in {
            'TestBase', 'TestPeewee', 'TestPeeweeSimple', 'TestSQLAlchemy', 'TestSQLAlchemySimple'
        }:
            raise unittest.SkipTest("Skipping base class")
        super().setUpClass()

    def setup_database(self):
        raise NotImplementedError

    def setup_app(self):
        raise NotImplementedError

    def init(self):
        raise NotImplementedError

    def test_basic(self):
        self.init()

        user_id = self.post_user(email='user0@example.com')
        user = self.get_user(user_id)
        self.get_user(0, expected_status_code=404)
        users = self.get_many_users()
        assert len(users) == 1

        self.patch_user(user_id)
        self.patch_user(0, expected_status_code=403)

        self.post_duplicate_user()

        self.post_user_address_null_street(user_id)
        self.post_user_address(user_id, '123 street')
        self.post_user_address_wrong_primary_key(user_id)


        self.delete_user(user_id)
        self.delete_user(0, expected_status_code=403)

    def get_user(self, pk, expected_status_code=200):
        r = self.testclient.get(
            f'/api/users/{pk}/'
        )
        print('r.status_code is', r.status_code)
        assert r.status_code == expected_status_code, r.json

        if expected_status_code != 200:
            return r.json['error']

        return r.json

    def get_many_users(self, expected_status_code=200, **query_params):
        r = self.testclient.get(
            f'/api/users/',
            query_string=query_params
        )
        assert r.status_code == expected_status_code, r.json
        if expected_status_code != 200:
            return r.json['error']

        assert 'items' in r.json, r.json
        return r.json['items']

    def post_user(self, email, expected_status_code=201):
        r = self.testclient.post(
            '/api/users/',
            json={
                'email': email
            }
        )
        assert r.status_code == expected_status_code, r.json
        if expected_status_code != 201:
            return r.json['error']

        assert r.json.get('email') == email, r.json
        user_id = r.json.get('id')
        assert user_id

        return user_id

    def patch_user(self, user_id, expected_status_code=200):
        r = self.testclient.patch(
            f'/api/users/{user_id}',
            json={
                'email': 'user1_patched@example.com'
            }
        )

        assert r.status_code == expected_status_code, r.json

        if expected_status_code != 200:
            return r.json['error']

        assert r.json.get('email') == 'user1_patched@example.com', r.json
        assert r.json.get('id') == user_id

    def delete_user(self, user_id, expected_status_code=200):
        r = self.testclient.delete(
            f'/api/users/{user_id}/'
        )

        assert r.status_code == expected_status_code, r.json

        return r.json

    def post_duplicate_user(self):
        self.post_user('duplicate@example.com')
        error = self.post_user('duplicate@example.com', expected_status_code=400)
        assert error == {'email': 'Duplicates are not permitted.'}

    def post_user_address(self, user_id, street, expected_status_code=201):
        r = self.testclient.post(
            '/api/user-addresses/',
            json={
                'user_id': user_id,
                'street': street
            }
        )

        assert r.status_code == expected_status_code, r.json
        if expected_status_code != 201:
            return r.json['error']
        assert r.json.get('user_id', user_id)
        assert r.json.get('street', street)

    def post_user_address_wrong_primary_key(self, user_id):
        # Duplicate case
        error = self.post_user_address(user_id, '123 street', expected_status_code=400)
        assert error == {'user_id': 'Duplicates are not permitted.'}

        error = self.post_user_address(-1, '123 street', expected_status_code=404)
        assert error == self.user_id_foreign_key_error()

        error = self.post_user_address(None, '123 street', expected_status_code=400)
        assert error == {'user_id': ['Field may not be null.']}

    def user_id_foreign_key_error(self):
        return {'user_id': 'No resource with this value exists.'}

    def post_user_address_null_street(self, user_id):
        error = self.post_user_address(user_id, None, expected_status_code=400)
        assert error == {'street': ['Field may not be null.']}