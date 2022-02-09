"""
Test tap discovery
"""
import re

from tap_tester import menagerie, connections, runner

from base import BingAdsBaseTest


class DiscoveryTest(BingAdsBaseTest):
    """ Test the tap discovery """

    @staticmethod
    def name():
        return "tap_tester_bing_ads_discovery_test"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.
        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        streams_to_test = self.expected_streams()

        conn_id = self.create_connection()

        # Verify number of actual streams discovered match expected
        catalogs = menagerie.get_catalogs(conn_id)
        found_catalogs = [catalog for catalog in catalogs
                          if catalog.get('tap_stream_id') in streams_to_test]

        self.assertGreater(len(found_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))
        self.assertEqual(len(found_catalogs),
                         len(streams_to_test),
                         msg="Expected {} streams, actual was {} for connection {}, "
                             "actual {}".format(
                                 len(streams_to_test),
                                 len(found_catalogs),
                                 found_catalogs,
                                 conn_id))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(streams_to_test),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                schema = schema_and_metadata["annotated-schema"]

                # verify the stream level properties are as expected
                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_foreign_keys = self.expected_foreign_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                # As there is a discrepancy for replication key in existing tap's catalog, sync mode behavior and documentation
                # removing replication key "TimePeriod" from each report streams. Bug: TDL-15816
                if stream.endswith("_report"):
                    expected_replication_keys = expected_replication_keys - {"TimePeriod"}
                expected_required_keys = self.expected_required_fields()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys \
                    | expected_foreign_keys | expected_required_keys

                # verify replication key(s)
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])),
                    expected_replication_keys,
                    msg="expected replication key {} but actual is {}".format(
                        expected_replication_keys,
                        set(stream_properties[0].get(
                            "metadata", {self.REPLICATION_KEYS: None}).get(
                                self.REPLICATION_KEYS, []))))

                # verify primary key(s)
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                    expected_primary_keys,
                    msg="expected primary key {} but actual is {}".format(
                        expected_primary_keys,
                        set(stream_properties[0].get(
                            "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

                # # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # actual_replication_method = stream_properties[0].get(
                #     "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                # if stream_properties[0].get(
                #         "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []):

                #     self.assertTrue(actual_replication_method == self.INCREMENTAL,
                #                     msg="Expected INCREMENTAL replication "
                #                         "since there is a replication key")
                # else:
                #     self.assertTrue(actual_replication_method == self.FULL_TABLE,
                #                     msg="Expected FULL replication "
                #                         "since there is no replication key")

                # # verify the actual replication matches our expected replication method
                # self.assertEqual(
                #     self.expected_replication_method().get(stream, None),
                #     actual_replication_method,
                #     msg="The actual replication method {} doesn't match the expected {}".format(
                #         actual_replication_method,
                #         self.expected_replication_method().get(stream, None)))

                # END OF BUG SRCE-4315

                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_foreign_keys = self.expected_foreign_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_required_keys = self.expected_required_fields()[stream]
                # These streams does not include the field `_sdc_report_datetime` in their schema
                if stream in ['ads', 'ad_groups', 'campaigns', 'accounts']:
                    expected_automatic_fields = expected_primary_keys | expected_replication_keys \
                        | expected_foreign_keys | expected_required_keys
                # adding `_sdc_report_datetime` in the expected automatic fields list as there was 
                # a failure experienced for all the streams that the particular field was missing
                # in the expected fields.
                else:
                    expected_automatic_fields = expected_primary_keys | expected_replication_keys \
                        | expected_foreign_keys | expected_required_keys | {'_sdc_report_datetime'}

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in annotated schema.
                actual_automatic_fields = {key for key, value in schema["properties"].items()
                                           if value.get("inclusion") == "automatic"}
                self.assertEqual(expected_automatic_fields, actual_automatic_fields)


                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({value.get("inclusion") == "available" for key, value
                         in schema["properties"].items()
                         if key not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in annotated schema")

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in metadata.
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}
                
                self.assertEqual(expected_automatic_fields,
                                 actual_automatic_fields,
                                 msg="expected {} automatic fields but got {}".format(
                                     expected_automatic_fields,
                                     actual_automatic_fields))

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")
