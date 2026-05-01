"""Tests for TargetElasticsearch.__init__ config validation."""

import pytest

from target_elasticsearch.target import TargetElasticsearch


class TestAuthPairingValidation:
    def test_username_without_password_raises(self):
        with pytest.raises(ValueError, match="username.*password"):
            TargetElasticsearch(config={"username": "elastic"})

    def test_password_without_username_raises(self):
        with pytest.raises(ValueError, match="username.*password"):
            TargetElasticsearch(config={"password": "secret"})

    def test_api_key_without_id_raises(self):
        with pytest.raises(ValueError, match="api_key_id.*api_key"):
            TargetElasticsearch(config={"api_key": "k"})

    def test_api_key_id_without_key_raises(self):
        with pytest.raises(ValueError, match="api_key_id.*api_key"):
            TargetElasticsearch(config={"api_key_id": "id"})

    def test_full_basic_auth_pair_is_accepted(self):
        # Should construct without error
        TargetElasticsearch(config={"username": "elastic", "password": "secret"})

    def test_full_api_key_pair_is_accepted(self):
        TargetElasticsearch(config={"api_key_id": "id", "api_key": "k"})

    def test_no_auth_is_accepted(self):
        TargetElasticsearch(config={})
