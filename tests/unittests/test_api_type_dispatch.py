"""Unit tests for the REST/SOAP api_type dispatch logic in tap_bing_ads/__init__.py."""
import sys
import unittest
from unittest.mock import MagicMock, patch

import tap_bing_ads
from tap_bing_ads import get_api_type, run_rest, run_soap, main

DEFAULT_CONFIG = {
    "oauth_client_id": "id",
    "oauth_client_secret": "secret",
    "refresh_token": "refresh",
    "developer_token": "dev",
    "customer_id": "12345",
    "account_ids": "67890",
    "start_date": "2024-01-01T00:00:00Z",
}


class TestGetApiType(unittest.TestCase):
    """REST is the default/foundation transport; `api_type` opts into legacy SOAP."""

    def test_defaults_to_rest_when_unset(self):
        self.assertEqual(get_api_type({}), "rest")

    def test_explicit_rest(self):
        self.assertEqual(get_api_type({"api_type": "rest"}), "rest")

    def test_explicit_soap_is_case_insensitive(self):
        self.assertEqual(get_api_type({"api_type": "SOAP"}), "soap")
        self.assertEqual(get_api_type({"api_type": " Soap "}), "soap")

    def test_invalid_value_raises(self):
        with self.assertRaises(ValueError):
            get_api_type({"api_type": "xml"})


class TestRunSoap(unittest.TestCase):
    """api_type='soap' must still work for existing connections until 2027-01-31."""

    def test_raises_actionable_error_when_soap_extra_not_installed(self):
        # Simulate the `bingads`/`suds` optional extra not being installed by
        # forcing the import of tap_bing_ads.soap_tap to fail, regardless of
        # whether it happens to be installed in the environment running this
        # test (setting a sys.modules entry to None makes Python's import
        # system raise ImportError for that module).
        with patch.dict(sys.modules, {"tap_bing_ads.soap_tap": None}):
            with self.assertRaises(ImportError) as ctx:
                run_soap(DEFAULT_CONFIG, {}, catalog=None, is_discover=True)
        self.assertIn("tap-bing-ads[soap]", str(ctx.exception))

    def test_dispatches_to_soap_discover_when_available(self):
        fake_soap_tap = MagicMock()
        with patch.dict(sys.modules, {"tap_bing_ads.soap_tap": fake_soap_tap}):
            run_soap(DEFAULT_CONFIG, {}, catalog=None, is_discover=True)
        fake_soap_tap.run_discover.assert_called_once_with(DEFAULT_CONFIG)
        fake_soap_tap.run_sync.assert_not_called()

    def test_dispatches_to_soap_sync_when_available(self):
        fake_soap_tap = MagicMock()
        state = {"bookmarks": {}}
        catalog = MagicMock()
        with patch.dict(sys.modules, {"tap_bing_ads.soap_tap": fake_soap_tap}):
            run_soap(DEFAULT_CONFIG, state, catalog=catalog, is_discover=False)
        fake_soap_tap.run_sync.assert_called_once_with(DEFAULT_CONFIG, state, catalog)
        fake_soap_tap.run_discover.assert_not_called()


class TestRunRest(unittest.TestCase):
    """REST is the default transport and the foundation for new development."""

    @patch("tap_bing_ads.sync")
    @patch("tap_bing_ads.do_discover")
    @patch("tap_bing_ads.Client")
    def test_discover_mode(self, mock_client_cls, mock_do_discover, mock_sync):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        config = dict(DEFAULT_CONFIG)

        run_rest(config, None, {}, catalog=None, is_discover=True)

        mock_do_discover.assert_called_once_with(mock_client)
        mock_sync.assert_not_called()
        self.assertIn("end_date", config)

    @patch("tap_bing_ads.sync")
    @patch("tap_bing_ads.do_discover")
    @patch("tap_bing_ads.Client")
    def test_sync_mode(self, mock_client_cls, mock_do_discover, mock_sync):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client
        config = dict(DEFAULT_CONFIG)
        state = {"bookmarks": {}}
        catalog = MagicMock()

        run_rest(config, "/tmp/config.json", state, catalog=catalog, is_discover=False)

        mock_sync.assert_called_once_with(
            client=mock_client, config=config, catalog=catalog, state=state
        )
        mock_do_discover.assert_not_called()


class TestMainDispatch(unittest.TestCase):
    """main() must route to REST by default and to SOAP only when configured."""

    def _fake_args(self, config, discover=False, catalog=None, state=None):
        args = MagicMock()
        args.config = config
        args.state = state or {}
        args.discover = discover
        args.catalog = catalog
        args.config_path = "/tmp/config.json"
        return args

    @patch("tap_bing_ads.run_rest")
    @patch("tap_bing_ads.run_soap")
    @patch("tap_bing_ads.utils.parse_args")
    def test_defaults_to_rest(self, mock_parse_args, mock_run_soap, mock_run_rest):
        mock_parse_args.return_value = self._fake_args(dict(DEFAULT_CONFIG), discover=True)

        main()

        mock_run_rest.assert_called_once()
        mock_run_soap.assert_not_called()

    @patch("tap_bing_ads.run_rest")
    @patch("tap_bing_ads.run_soap")
    @patch("tap_bing_ads.utils.parse_args")
    def test_dispatches_to_soap_when_configured(self, mock_parse_args, mock_run_soap, mock_run_rest):
        config = dict(DEFAULT_CONFIG)
        config["api_type"] = "soap"
        mock_parse_args.return_value = self._fake_args(config, discover=True)

        main()

        mock_run_soap.assert_called_once()
        mock_run_rest.assert_not_called()

    @patch("tap_bing_ads.run_rest")
    @patch("tap_bing_ads.run_soap")
    @patch("tap_bing_ads.utils.parse_args")
    def test_no_discover_or_catalog_is_a_noop(self, mock_parse_args, mock_run_soap, mock_run_rest):
        mock_parse_args.return_value = self._fake_args(
            dict(DEFAULT_CONFIG), discover=False, catalog=None
        )

        main()

        mock_run_soap.assert_not_called()
        mock_run_rest.assert_not_called()


if __name__ == "__main__":
    unittest.main()
