import csv
import io
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Tuple
from zipfile import ZipFile

import arrow
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    write_state,
)

from tap_bing_ads.client import REPORTING_BASE_URL, Client
from tap_bing_ads.exceptions import (
    BingAdsInvalidDateRangeEnd,
    BingAdsNoMeasureSelected,
    BingAdsInvalidFieldSelection,
    BingAdsReportError,
    BingAdsForbiddenError
)

LOGGER = get_logger()

# ~2 hour polling timeout at 5-second intervals
MAX_REPORT_POLLS = 1440
REPORT_POLL_SLEEP = 5

# Default sync window and conversion window (days)
DEFAULT_REPORT_MAX_DAYS = 30
DEFAULT_CONVERSION_WINDOW = -30

# Fields added by the tap (not from API) — excluded from report column requests
SDC_FIELDS = ["_sdc_report_datetime"]


class BaseStream(ABC):
    """
    Abstract base class for all core entity streams.
    Subclasses declare `children` (list of stream class names) and implement
    `get_records(parent_record)` to yield raw API records.
    """
    url_endpoint = ""
    path = ""
    parent: Optional[str] = None
    children: List[str] = []
    data_key = ""
    parent_bookmark_key = ""
    http_method = "POST"
    bookmark_value = None

    def __init__(self, client: Client, catalog_entry=None) -> None:
        self.client = client
        self.catalog_entry = catalog_entry
        self.schema = catalog_entry.schema.to_dict() if catalog_entry else {}
        self.metadata = metadata.to_map(catalog_entry.metadata) if catalog_entry else {}
        self.child_to_sync: List["BaseStream"] = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
            - state (dict): represents the state file for the tap.
            - transformer (object): A Object of the singer.transformer class.
            - parent_obj (dict): The parent object for the stream.

        Returns:
            - dict: The updated state after the sync.

        Docs:
            - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def is_selected(self) -> bool:
        return metadata.get(self.metadata, (), "selected")

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, state: Dict = None, parent_obj: Dict = None, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def modify_object(self, record: Dict, parent_obj: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream
        """
        return self.url_endpoint

    def update_data_payload(self, parent_obj: Dict = None, **kwargs) -> Dict:
        """
        Constructs the JSON body payload for the API request.
        """
        self.data_payload.update(kwargs)
        return self.data_payload

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        """Yield records from the API response."""
        response = self.client.make_request(
            method=self.http_method,
            url=self.url_endpoint,
            params=self.params if self.params else None,
            json_body=self.data_payload if self.http_method == "POST" else None,
            account_id=parent_obj.get("account_id") or parent_obj.get("Id") if parent_obj else None
        )
        if self.data_key:
            raw_data = response.get(self.data_key)
        else:
            raw_data = response
        if not raw_data:
            return
        if isinstance(raw_data, list):
            yield from raw_data
        else:
            yield raw_data

    def check_access(self) -> bool:
        """
        Verify that the configured API credentials have read access to this stream.

        Uses the first ``account_id`` from config when probing the endpoint so the
        request is valid.  Returns False only on HTTP 403; all other errors propagate.

        Concrete subclasses that require a parent ID in their payload (e.g. campaigns
        needs AccountId, ad_groups needs CampaignId) must override this method.
        """

        account_id = self.client.config.get("account_ids", "").split(",")[0].strip()
        url = self.get_url_endpoint()
        self.update_data_payload()

        try:
            self.client.make_request(
                method=self.http_method,
                url=url,
                json_body=self.data_payload if self.http_method == "POST" else None,
                account_id=account_id or None,
            )
            return True
        except BingAdsForbiddenError as exc:
            LOGGER.warning(
                "Unauthorized stream: %s — excluding from catalog. Error: '%s'",
                self.tap_stream_id,
                str(exc),
            )
            return False


class IncrementalStream(BaseStream):
    """
    Base class for INCREMENTAL streams.
    Filters by bookmark, writes bookmark after sync, and drives child streams.
    """

    replication_method = "INCREMENTAL"

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )

    def sync(
            self,
            state: Dict,
            transformer: Transformer,
            parent_obj: Optional[Dict] = None,
            **kwargs
        ) -> int:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_params(state=state, parent_obj=parent_obj)
        self.update_data_payload(parent_obj=parent_obj)

        count = 0
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_obj=parent_obj):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                record_bookmark = transformed_record.get(self.replication_keys[0])

                if record_bookmark >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()
                        count += 1

                    if record_bookmark > current_max_bookmark_date:
                        current_max_bookmark_date = record_bookmark

                    for child in self.child_to_sync:
                        child.sync(state, transformer, parent_obj=record)

        if current_max_bookmark_date and current_max_bookmark_date != bookmark_date:
            self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)

        return count

class FullTableStream(BaseStream):
    """
    Base class for FULL_TABLE streams (campaigns, ad_groups, ads).
    Writes every record and drives child streams per record.
    """

    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, state: Dict, transformer: Transformer, parent_record: Optional[Dict] = None, **kwargs) -> int:
        self.write_schema()

        count = 0
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_record):
                transformed = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                    count += 1

                # Pass raw record to children so injected context fields (_account_id, etc.) are preserved
                for child in self.child_to_sync:
                    child.sync(state, transformer, parent_record=record)

        return count

class BaseReport(ABC):
    """
    Abstract base class for asynchronous report streams.

    Flow:
      1. Build a JSON report request body.
      2. POST to ``/Reporting/v13/GenerateReport/Submit`` → receive ``ReportRequestId``.
      3. Poll ``/Reporting/v13/GenerateReport/Poll`` until status == 'Success' or 'Error'.
      4. Download the ZIP, parse the CSV, write Singer records.
    """

    # Subclasses must override these
    tap_stream_id: str = ""
    report_name: str = ""
    key_properties: List[str] = []
    replication_method: str = "INCREMENTAL"
    replication_keys: List[str] = ["TimePeriod"]
    parent: Optional[str] = "accounts"

    # Required columns (always included regardless of selection)
    required_columns: List[str] = ["TimePeriod", "AccountId"]
    # Measure columns required for a valid probe request (at least one measure is mandatory)
    required_measure_columns: List[str] = ["Clicks"]
    # Extra columns required for this specific report (overridden by subclass)
    report_specific_columns: List[str] = []

    # Reporting API endpoint paths (relative to REPORTING_BASE_URL)
    report_submit_path: str = "GenerateReport/Submit"
    report_poll_path: str = "GenerateReport/Poll"

    def __init__(self, client: Client, catalog_entry=None) -> None:
        self.client = client
        self.catalog_entry = catalog_entry
        self.schema = catalog_entry.schema.to_dict() if catalog_entry else {}
        self.metadata = metadata.to_map(catalog_entry.metadata) if catalog_entry else {}

    def is_selected(self) -> bool:
        return metadata.get(self.metadata, (), "selected")

    def write_schema(self) -> None:
        write_schema(self.tap_stream_id, self.schema, self.key_properties)

    def check_access(self) -> bool:
        """
        Probe the report submit endpoint to verify the credentials have access.

        Builds a fully-formed request body using the stream's own ``report_name``,
        ``required_columns``, ``report_specific_columns``, and the first
        ``account_id`` from config, so the API validates both credentials and the
        report type.  Only HTTP 403 means genuinely unauthorised.
        """
        import arrow

        account_id = self.client.config.get("account_ids", "").split(",")[0].strip()
        url = f"{REPORTING_BASE_URL}/{self.report_submit_path}"

        # Use yesterday as a minimal 1-day window so the Time fields are valid.
        end_date = arrow.utcnow().shift(days=-1)
        start_date = end_date
        columns = list(dict.fromkeys(
            self.required_columns + self.report_specific_columns + self.required_measure_columns
        ))
        body = self._build_report_request(account_id, columns, start_date, end_date)

        try:
            self.client.make_request("POST", url, json_body=body,
                                     account_id=account_id or None)
            return True
        except BingAdsForbiddenError as exc:
            LOGGER.warning(
                "Unauthorized report stream: %s — excluding from catalog. Error: '%s'",
                self.tap_stream_id,
                str(exc),
            )
            return False

    def get_selected_columns(self) -> List[str]:
        """Return selected columns for the report (excludes SDC metadata fields)."""
        selected = []
        for prop in self.schema.get("properties", {}):
            if prop in SDC_FIELDS:
                continue
            inclusion = metadata.get(self.metadata, ("properties", prop), "inclusion")
            is_selected = metadata.get(self.metadata, ("properties", prop), "selected")
            if inclusion == "automatic" or is_selected is True:
                selected.append(prop)

        # Validate mutually exclusive selections
        self._check_field_exclusions(selected)
        return selected

    def _check_field_exclusions(self, selected: List[str]) -> None:
        """Raise BingAdsInvalidFieldSelection if mutually exclusive fields are selected."""
        from tap_bing_ads.exclusions import EXCLUSIONS
        exclusion_rules = EXCLUSIONS.get(self.report_name, [])
        conflicts: Dict[str, List[str]] = {}
        for rule in exclusion_rules:
            attrs = set(rule.get("Attributes", []))
            share_stats = set(rule.get("ImpressionSharePerformanceStatistics", []))
            for field in selected:
                if field in attrs:
                    bad = [f for f in selected if f in share_stats]
                    if bad:
                        conflicts[field] = conflicts.get(field, []) + bad
                if field in share_stats:
                    bad = [f for f in selected if f in attrs]
                    if bad:
                        conflicts[field] = conflicts.get(field, []) + bad
        if conflicts:
            import json
            raise BingAdsInvalidFieldSelection(
                f"Mutually exclusive fields selected: {json.dumps(conflicts, indent=2)}"
            )

    def _build_report_request(
        self, account_id: str, columns: List[str], start_date: arrow.Arrow, end_date: arrow.Arrow
    ) -> Dict:
        """Build the JSON body for a report generation request."""
        return {
            "ReportRequest": {
                "Type": f"{self.report_name}Request",
                "ReportName": self.report_name,
                "Format": "Csv",
                "FormatVersion": "2.0",
                "Language": "English",
                "ExcludeColumnHeaders": False,
                "ExcludeReportHeader": True,
                "ExcludeReportFooter": True,
                "Aggregation": "Daily",
                "Scope": {
                    "AccountIds": [int(account_id)]
                },
                "Time": {
                    "CustomDateRangeStart": {
                        "Day": start_date.day,
                        "Month": start_date.month,
                        "Year": start_date.year,
                    },
                    "CustomDateRangeEnd": {
                        "Day": end_date.day,
                        "Month": end_date.month,
                        "Year": end_date.year,
                    },
                    "ReportTimeZone": "GreenwichMeanTimeDublinEdinburghLisbonLondon",
                },
                "Columns": columns,
            }
        }

    def _submit_report(self, account_id: str, body: Dict) -> str:
        """POST the report request and return the ReportRequestId."""
        url = f"{REPORTING_BASE_URL}/{self.report_submit_path}"
        resp = self.client.make_request("POST", url, json_body=body, account_id=account_id)
        request_id = resp.get("ReportRequestId") or resp.get("reportRequestId")
        if not request_id:
            raise BingAdsReportError(
                f"No ReportRequestId in response for {self.report_name}: {resp}"
            )
        return request_id

    def _poll_report(self, account_id: str, request_id: str) -> Optional[str]:
        """
        Poll until the report is ready.

        Returns the download URL on success or None when there is no data.
        Raises BingAdsReportError on Error status.
        """
        url = f"{REPORTING_BASE_URL}/{self.report_poll_path}"
        with metrics.job_timer("generate_report"):
            for attempt in range(1, MAX_REPORT_POLLS + 1):
                resp = self.client.make_request(
                    "POST",
                    url,
                    json_body={"ReportRequestId": request_id},
                    account_id=account_id,
                )
                status_obj = resp.get("ReportRequestStatus") or resp
                status = status_obj.get("Status") or status_obj.get("status", "")

                if status == "Error":
                    raise BingAdsReportError(
                        f"Report {self.report_name} (id={request_id}) returned Error status. "
                        f"Response: {resp}"
                    )
                if status == "Success":
                    return (
                        status_obj.get("ReportDownloadUrl")
                        or status_obj.get("reportDownloadUrl")
                    )
                if attempt == MAX_REPORT_POLLS:
                    LOGGER.warning("Report polling timed out: %s", self.report_name)
                    return None

                LOGGER.info(
                    "Report %s: status=%s, poll %d/%d. Sleeping %ss.",
                    self.report_name, status, attempt, MAX_REPORT_POLLS, REPORT_POLL_SLEEP
                )
                time.sleep(REPORT_POLL_SLEEP)
        return None

    def _stream_report_csv(
        self, stream_name: str, download_url: str, report_time: str, sdc_fields: Dict
    ) -> int:
        """Download the report ZIP, parse its CSV, write Singer records. Returns row count."""
        content = self.client.download_file(download_url)
        count = 0
        with ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as binary_file:
                with io.TextIOWrapper(binary_file, encoding="utf-8-sig") as text_file:
                    reader = csv.DictReader(text_file)
                    with metrics.record_counter(stream_name) as counter:
                        for row in reader:
                            row = {k.strip(): v.strip() for k, v in row.items() if k}
                            row["_sdc_report_datetime"] = report_time
                            self._type_row(row)
                            write_record(stream_name, row)
                            counter.increment()
                            count += 1
        return count

    def _type_row(self, row: Dict) -> None:
        """Cast report column values to their declared types in-place."""
        schema_props = self.schema.get("properties", {})
        for field, value in row.items():
            if not value and value != 0:
                row[field] = None
                continue
            prop = schema_props.get(field, {})
            prop_type = prop.get("type", "string")
            if isinstance(prop_type, str):
                prop_type = [prop_type]

            if "integer" in prop_type:
                try:
                    row[field] = int(value.replace(",", ""))
                except (ValueError, AttributeError):
                    # Bing Ads uses non-numeric sentinels ("-", "--", "N/A", etc.)
                    # for metrics with no applicable data. Treat them as zero.
                    row[field] = 0
            elif "number" in prop_type:
                try:
                    row[field] = float(value.replace("%", "").replace(",", ""))
                except (ValueError, AttributeError):
                    # Same sentinel handling as integer above.
                    row[field] = 0.0
            elif prop.get("format") == "date-time":
                try:
                    row[field] = arrow.get(value).isoformat()
                except Exception:
                    pass

    def sync(self, state: Dict, account_id: str, config: Dict) -> int:
        """
        Sync one report stream for one account over the configured date window.
        Returns total records written.
        """
        report_max_days = int(config.get("report_max_days", DEFAULT_REPORT_MAX_DAYS))
        conversion_window = int(config.get("conversion_window", DEFAULT_CONVERSION_WINDOW))
        state_key = f"{account_id}_{self.tap_stream_id}"

        config_start = arrow.get(config.get("start_date")).floor("day")
        config_end = arrow.get(config.get("end_date", arrow.now().isoformat())).floor("day")
        conversion_min_date = arrow.now().floor("day").shift(days=conversion_window)

        bookmark_date = get_bookmark(state, state_key, "date")
        if bookmark_date:
            start_date = arrow.get(bookmark_date).floor("day").shift(days=1)
        else:
            start_date = config_start
        start_date = min(start_date, conversion_min_date)
        end_date = min(config_end, arrow.now().floor("day"))

        if start_date > end_date:
            LOGGER.info(
                "No date range to sync for %s (account %s): start=%s end=%s",
                self.tap_stream_id, account_id, start_date, end_date
            )
            return 0

        columns = self.get_selected_columns()
        if not columns:
            LOGGER.warning("No columns selected for %s — skipping.", self.tap_stream_id)
            return 0

        # Check that at least one metric column is present
        from tap_bing_ads.reports import METRIC_COLUMNS
        if not any(c in METRIC_COLUMNS for c in columns):
            raise BingAdsNoMeasureSelected(
                f"Stream '{self.tap_stream_id}' has no metric columns selected. "
                "Select at least one metric (e.g. Impressions, Clicks, Spend)."
            )

        total = 0
        current_start = start_date
        while current_start <= end_date:
            current_end = min(current_start.shift(days=report_max_days), end_date)
            try:
                count = self._sync_interval(
                    state, state_key, account_id, columns, current_start, current_end
                )
                total += count
                current_start = current_end.shift(days=1)
            except BingAdsInvalidDateRangeEnd:
                LOGGER.warning(
                    "Date range end outside retention for %s — skipping window.",
                    self.tap_stream_id
                )
                current_start = current_end.shift(days=1)
            except BingAdsNoMeasureSelected:
                LOGGER.warning("No measure columns for %s — skipping.", self.tap_stream_id)
                break
            except BingAdsInvalidFieldSelection as exc:
                LOGGER.warning(str(exc))
                break
            except BingAdsReportError as exc:
                LOGGER.warning("Report error for %s: %s — skipping interval.", self.tap_stream_id, exc)
                current_start = current_end.shift(days=1)

        return total

    def _sync_interval(
        self,
        state: Dict,
        state_key: str,
        account_id: str,
        columns: List[str],
        start_date: arrow.Arrow,
        end_date: arrow.Arrow,
    ) -> int:
        """Sync a single date-window chunk; returns record count."""
        self.write_schema()
        report_time = arrow.now().isoformat()

        # Resume from saved request id if available
        request_id = get_bookmark(state, state_key, "request_id")
        if not request_id:
            body = self._build_report_request(account_id, columns, start_date, end_date)
            request_id = self._submit_report(account_id, body)
            write_bookmark(state, state_key, "request_id", request_id)
            write_state(state)

        LOGGER.info(
            "Polling report %s (account=%s, %s to %s, id=%s)",
            self.report_name, account_id, start_date.date(), end_date.date(), request_id
        )

        try:
            download_url = self._poll_report(account_id, request_id)
        except BingAdsReportError:
            # API-level error — re-raise immediately, do not regenerate
            raise
        except Exception as exc:
            # Network/connection error or expired request ID — regenerate once
            LOGGER.info("Regenerating report request after transient poll error: %s", exc)
            body = self._build_report_request(account_id, columns, start_date, end_date)
            request_id = self._submit_report(account_id, body)
            write_bookmark(state, state_key, "request_id", request_id)
            write_state(state)
            download_url = self._poll_report(account_id, request_id)

        write_bookmark(state, state_key, "request_id", None)

        count = 0
        if download_url:
            LOGGER.info(
                "Streaming report %s (account=%s, %s to %s)",
                self.report_name, account_id, start_date.date(), end_date.date()
            )
            count = self._stream_report_csv(
                self.tap_stream_id, download_url, report_time, {}
            )
        else:
            LOGGER.info(
                "No data for report %s (account=%s, %s to %s)",
                self.report_name, account_id, start_date.date(), end_date.date()
            )

        write_bookmark(state, state_key, "date", end_date.isoformat())
        write_state(state)
        return count
