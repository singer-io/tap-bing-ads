import os
import uuid
import unittest
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from base import BingAdsBaseTest

import singer
from singer import metadata
LOGGER = singer.get_logger()


class BingAdsSyncRows(BingAdsBaseTest):

    @staticmethod
    def name():
        return "tap_tester_bing_ads_sync_rows"

    def expected_sync_streams(self):
        return {
            'accounts',
        }

    def expected_pks(self):
        return {
            'accounts': {'Id'},
        }

    def test_run(self):
        # Select our catalogs
        # found_catalogs = menagerie.get_catalogs(conn_id)
        # our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        # for c in our_catalogs:
        #     c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
        #     c_metadata = metadata.to_map(c_annotated['metadata'])
        #     connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], [])

        conn_id = self.create_connection()

        # Clear state before our run
        menagerie.set_state(conn_id, {})
        # Select a stream
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  sum(record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Ensure all records have a value for PK(s)
        records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            messages = records.get(stream, {}).get('messages')
            for m in messages:
                pk_set = self.expected_pks()[stream]
                for pk in pk_set:
                    self.assertIsNotNone(m.get('data', {}).get(pk), msg="oh no! {}".format(m))

        bookmarks = menagerie.get_state(conn_id)['bookmarks']

        for stream in self.expected_sync_streams():
            self.assertTrue(stream in bookmarks)
