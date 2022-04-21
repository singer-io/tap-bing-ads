# tap-bing-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- `tap-bing-ads` works together with any other [Singer Target](https://singer.io) to move data from BingAds API to any target destination.

## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://docs.microsoft.com/en-us/advertising/guides/authentication-oauth?view=bingads-13) authentication and a cutoff date for syncing historical data. See [config.sample.json](config.sample.json) for an example.

To run the discover mode of `tap-bing-ads` with the configuration file, use this command:

```bash
$ tap-bing-ads -c my-config.json -d
```

To run he sync mode of `tap-bing-ads` with the catalog file, use the command:

```bash
$ tap-bing-ads -c my-config.json --catalog catalog.json
```

## Metadata Reference

tap-bing-ads uses some custom metadata keys:

* `fieldExclusions` - Indicates which other fields may not be selected when this field is selected. If you invoke the tap with selections that violate fieldExclusion rules, it is likely that the tap will fail.

---

Copyright &copy; 2017 Stitch
