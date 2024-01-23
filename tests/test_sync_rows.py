import os
import uuid
import unittest
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from base import BingAdsBaseTest


class BingAdsSyncRows(BingAdsBaseTest):

    @staticmethod
    def name():
        return "tap_tester_bing_ads_sync_rows"

    def expected_sync_streams(self):
        return {
            'accounts',
            # 'ad_extension_detail_report',
            # 'ad_extension_detail_report',
            # 'ad_group_performance_report',
            'ad_groups',
            # 'ad_performance_report',
            'ads',
            # 'age_gender_demographic_report',
            # 'audience_performance_report',
            # 'campaign_performance_report',
            'campaigns',
            # 'geographic_performance_report',
            # 'goals_and_funnels_report',
            # 'keyword_performance_report',
            # 'search_query_performance_report',
        }

    def expected_pks(self):
        primary_keys = self.expected_primary_keys()
        return {
            stream: primary_keys.get(stream)
            for stream in self.expected_sync_streams()
        }

    def test_run(self):
        # Select our catalogs
        # found_catalogs = menagerie.get_catalogs(conn_id)
        # our_catalogs = [c for c in found_catalogs
        #                 if c.get('tap_stream_id') in self.expected_sync_streams()]
        # for c in our_catalogs:
        #     c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
        #     c_metadata = metadata.to_map(c_annotated['metadata'])
        #     connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], [])

        conn_id = self.create_connection()

        # Clear state before our run
        menagerie.set_state(conn_id, {})
        # Select a stream
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        state = menagerie.get_state(conn_id)
        record_count_by_stream = self.run_and_verify_sync(conn_id, state)

        # Ensure all records have a value for PK(s)
        records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            messages = records.get(stream, {}).get('messages')
            for m in messages:
                pk_set = self.expected_pks()[stream]
                for pk in pk_set:
                    self.assertIsNotNone(m.get('data', {}).get(pk), msg="oh no! {}".format(m))

        bookmarks = menagerie.get_state(conn_id)['bookmarks']

        replication_methods = self.expected_replication_method()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_method = replication_methods.get(stream)
                if replication_method is self.INCREMENTAL:
                    self.assertTrue(stream in bookmarks)

                elif replication_method is self.FULL_TABLE:
                    self.assertTrue(stream not in bookmarks)

                else:
                    raise NotImplementedError("stream {} has an invalid replication "
                                              "method {}".format(stream, replication_method)
                    )
