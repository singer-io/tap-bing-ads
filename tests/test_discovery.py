                #     self.expected_replication_keys()[stream],
                #     msg="expected replication key {} but actual is {}".format(
                #         self.expected_replication_keys()[stream],
                #         set(stream_properties[0].get(
                #             "metadata", {self.REPLICATION_KEYS: None}).get(
                #                 self.REPLICATION_KEYS, []))))

                # # verify primary key(s)
                # self.assertEqual(
                #     set(stream_properties[0].get(
                #         "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                #     self.expected_primary_keys()[stream],
                #     msg="expected primary key {} but actual is {}".format(
                #         self.expected_primary_keys()[stream],
                #         set(stream_properties[0].get(
                #             "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

                # # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # actual_replication_method = stream_properties[0].get(
                #     "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                # if stream_properties[0].get(
                #         "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []):

                #     self.assertTrue(actual_replication_method == self.INCREMENTAL,
                #                     msg="Expected INCREMENTAL replication "
                #                         "since there is a replication key")
                # else:
                #     self.assertTrue(actual_replication_method == self.FULL,
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
                expected_foreign_keys = self.expected_foreign_keys()[stream] # TODO add foreign keys to expectations
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_required_keys = self.expected_required_fields()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys \
                    | expected_foreign_keys | expected_required_keys

                # BUG | https://stitchdata.atlassian.net/browse/SRCE-4313
                #       Uncomment from this point forward to see test failure and address bug

                # # verify that primary, replication and foreign keys
                # # are given the inclusion of automatic in annotated schema.
                # actual_automatic_fields = {key for key, value in schema["properties"].items()
                #                            if value.get("inclusion") == "automatic"}
                # self.assertEqual(expected_automatic_fields, actual_automatic_fields)
                         

                # # verify that all other fields have inclusion of available
                # # This assumes there are no unsupported fields for SaaS sources
                # self.assertTrue(
                #     all({value.get("inclusion") == "available" for key, value
                #          in schema["properties"].items()
                #          if key not in actual_automatic_fields}),
                #     msg="Not all non key properties are set to available in annotated schema")

                # # verify that primary, replication and foreign keys
                # # are given the inclusion of automatic in metadata.
                # actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                #                            for item in metadata
                #                            if item.get("metadata").get("inclusion") == "automatic"}
                # self.assertEqual(expected_automatic_fields,
                #                  actual_automatic_fields,
                #                  msg="expected {} automatic fields but got {}".format(
                #                      expected_automatic_fields,
                #                      actual_automatic_fields))

                # # verify that all other fields have inclusion of available
                # # This assumes there are no unsupported fields for SaaS sources
                # self.assertTrue(
                #     all({item.get("metadata").get("inclusion") == "available"
                #          for item in metadata
                #          if item.get("breadcrumb", []) != []
                #          and item.get("breadcrumb", ["properties", None])[1]
                #          not in actual_automatic_fields}),
                #     msg="Not all non key properties are set to available in metadata")
