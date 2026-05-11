import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from hubspot import HubSpot
from hubspot.crm.deals.models import Filter, FilterGroup, PublicObjectSearchRequest


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

load_dotenv()

HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
ETL_CONFIG_PATH = os.getenv("ETL_CONFIG_PATH", "config/hubspot_to_sheets.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def validate_env():
    missing = [
        name
        for name, value in {
            "HUBSPOT_ACCESS_TOKEN": HUBSPOT_ACCESS_TOKEN,
            "GOOGLE_SERVICE_ACCOUNT_JSON": GOOGLE_SA_JSON,
            "GOOGLE_SHEET_ID": GOOGLE_SHEET_ID,
        }.items()
        if not value
    ]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def load_config():
    config_path = Path(ETL_CONFIG_PATH)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    jobs = config.get("jobs", [config])
    for index, job in enumerate(jobs, start=1):
        required = ["primary_object", "primary_properties", "output_columns"]
        missing = [key for key in required if key not in job]
        if missing:
            raise ValueError(
                f"Missing required config keys for job {index}: {', '.join(missing)}"
            )

    return config


def get_month_range():
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    end = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)
    return start, end


def get_month_range_ms():
    start, end = get_month_range()
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def get_sheets_client():
    try:
        info = json.loads(GOOGLE_SA_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)
    except json.JSONDecodeError:
        pass

    if not os.path.isfile(GOOGLE_SA_JSON):
        raise FileNotFoundError(f"JSON file not found: {GOOGLE_SA_JSON}")

    creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=SCOPES)
    return gspread.authorize(creds)


def get_pipeline_stage_maps(client, object_type):
    pipeline_map = {}
    stage_map = {}

    try:
        pipelines = client.crm.pipelines.pipelines_api.get_all(object_type=object_type)
    except Exception as exc:
        log.warning("Could not fetch pipelines for %s: %s", object_type, exc)
        return pipeline_map, stage_map

    for pipeline in pipelines.results:
        pipeline_map[pipeline.id] = pipeline.label
        for stage in pipeline.stages:
            stage_map[stage.id] = stage.label

    return pipeline_map, stage_map


def get_stage_ids_by_label(stage_map, label):
    return [stage_id for stage_id, stage_label in stage_map.items() if stage_label == label]


def get_owner_map(client):
    owner_map = {}
    after = None

    while True:
        response = client.crm.owners.owners_api.get_page(limit=100, after=after)

        for owner in response.results:
            first_name = getattr(owner, "first_name", "") or ""
            last_name = getattr(owner, "last_name", "") or ""
            full_name = f"{first_name} {last_name}".strip()
            owner_map[str(owner.id)] = full_name or getattr(owner, "email", "") or str(owner.id)

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if not after:
            break

    return owner_map


def replace_dynamic_value(value):
    start, end = get_month_range()
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    replacements = {
        "{{month_start_ms}}": str(start_ms),
        "{{today_end_ms}}": str(end_ms),
        "{{month_start_iso}}": start.isoformat(),
        "{{today_end_iso}}": end.isoformat(),
    }

    if isinstance(value, str):
        return replacements.get(value, value)

    return value


def build_filters(filter_configs, stage_map):
    filters = []

    for item in filter_configs:
        operator = item["operator"]

        if "stage_label" in item:
            stage_ids = get_stage_ids_by_label(stage_map, item["stage_label"])
            if not stage_ids:
                log.warning("No stage IDs found for label %s", item["stage_label"])
            filters.append(
                Filter(
                    property_name=item["property"],
                    operator=operator,
                    values=stage_ids,
                )
            )
            continue

        kwargs = {
            "property_name": item["property"],
            "operator": operator,
        }

        if "value" in item:
            kwargs["value"] = replace_dynamic_value(item["value"])
        if "high_value" in item:
            kwargs["high_value"] = replace_dynamic_value(item["high_value"])
        if "values" in item:
            kwargs["values"] = [replace_dynamic_value(value) for value in item["values"]]

        filters.append(Filter(**kwargs))

    return filters


def fetch_primary_objects(client, config, stage_map):
    object_type = config["primary_object"]
    page_size = int(config.get("page_size", 100))
    properties = config.get("primary_properties", [])
    filters = build_filters(config.get("filters", []), stage_map)
    all_results = []
    after = None

    log.info("Fetching %s", object_type)

    while True:
        request = PublicObjectSearchRequest(
            filter_groups=[FilterGroup(filters=filters)] if filters else [],
            properties=properties,
            limit=page_size,
            after=after,
        )

        response = client.crm.objects.search_api.do_search(
            object_type=object_type,
            public_object_search_request=request,
        )
        all_results.extend(response.results)

        log.info("Fetched %d records (total: %d)", len(response.results), len(all_results))

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if not after:
            break

    return all_results


def get_associated_object_ids(client, from_object_type, from_object_id, to_object_type):
    try:
        response = client.crm.associations.v4.basic_api.get_page(
            object_type=from_object_type,
            object_id=str(from_object_id),
            to_object_type=to_object_type,
            limit=100,
        )

        ids = []
        for item in response.results:
            if hasattr(item, "to_object_id"):
                ids.append(str(item.to_object_id))
            elif hasattr(item, "id"):
                ids.append(str(item.id))

        return ids
    except Exception as exc:
        log.warning(
            "Could not fetch %s associations for %s %s: %s",
            to_object_type,
            from_object_type,
            from_object_id,
            exc,
        )
        return []


def batch_read_objects(client, object_type, object_ids, properties):
    unique_ids = list({str(object_id) for object_id in object_ids if object_id})
    if not unique_ids:
        return {}

    records = {}
    chunk_size = 100

    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]

        try:
            response = client.crm.objects.batch_api.read(
                object_type=object_type,
                batch_read_input_simple_public_object_id={
                    "properties": properties,
                    "inputs": [{"id": object_id} for object_id in chunk],
                },
            )

            for result in response.results:
                records[str(result.id)] = result.properties or {}
        except Exception as exc:
            log.warning("Could not batch fetch %s: %s", object_type, exc)

    return records


def format_value(value, column):
    if value is None:
        return ""

    if column.get("format") == "datetime":
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return ""
        return parsed.strftime("%Y-%m-%d %H:%M:%S")

    if column.get("format") == "date":
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return ""
        return parsed.strftime(column.get("date_format", "%-m/%-d/%Y"))

    return str(value)


def apply_lookup(value, lookup, pipeline_map, stage_map, owner_map):
    if not value:
        return ""

    value = str(value)
    if lookup == "pipeline":
        return pipeline_map.get(value, value)
    if lookup == "stage":
        return stage_map.get(value, value)
    if lookup == "owner":
        return owner_map.get(value, value)

    return value


def get_column_value(column, primary_props, associated_props, pipeline_map, stage_map, owner_map):
    source = column.get("source", "primary")
    property_name = column["property"]

    if source == "primary":
        value = primary_props.get(property_name, "")
    else:
        value = associated_props.get(source, {}).get(property_name, "")

    value = apply_lookup(
        value,
        column.get("lookup"),
        pipeline_map,
        stage_map,
        owner_map,
    )
    return format_value(value, column)


def transform_rows(primary_records, client, config, pipeline_map, stage_map, owner_map):
    primary_object = config["primary_object"]
    association_configs = config.get("associations", [])
    first_association = association_configs[0] if association_configs else None
    output_columns = config["output_columns"]
    rows = []

    association_map = {}
    associated_records_by_object = {}

    if first_association:
        to_object = first_association["object"]
        all_associated_ids = []
        log.info("Fetching %s associations", to_object)

        for index, record in enumerate(primary_records, start=1):
            record_id = str(record.id)
            ids = get_associated_object_ids(client, primary_object, record_id, to_object)
            association_map[record_id] = ids
            all_associated_ids.extend(ids)

            if index % 10 == 0:
                log.info("Processed associations for %d / %d records", index, len(primary_records))

        associated_records_by_object[to_object] = batch_read_objects(
            client,
            to_object,
            all_associated_ids,
            first_association.get("properties", []),
        )

    for record in primary_records:
        record_id = str(record.id)
        primary_props = record.properties or {}
        associated_ids = association_map.get(record_id, [])

        if not associated_ids:
            rows.append(
                {
                    column["header"]: get_column_value(
                        column,
                        primary_props,
                        {},
                        pipeline_map,
                        stage_map,
                        owner_map,
                    )
                    for column in output_columns
                }
            )
            continue

        for associated_id in associated_ids:
            associated_props = {}
            if first_association:
                source_name = first_association["object"].rstrip("s")
                associated_props[source_name] = associated_records_by_object[
                    first_association["object"]
                ].get(str(associated_id), {})

            rows.append(
                {
                    column["header"]: get_column_value(
                        column,
                        primary_props,
                        associated_props,
                        pipeline_map,
                        stage_map,
                        owner_map,
                    )
                    for column in output_columns
                }
            )

    headers = [column["header"] for column in output_columns]
    df = pd.DataFrame(rows, columns=headers).fillna("")
    return aggregate_rows(df, config).fillna("").astype(str)


def aggregate_rows(df, config):
    aggregate_config = config.get("aggregate")
    if df.empty or not aggregate_config:
        return df

    group_by = aggregate_config.get("group_by", [])
    aggregations = aggregate_config.get("aggregations", [])
    output_order = aggregate_config.get("output_order", group_by)

    if not group_by or not aggregations:
        return df

    missing_group_cols = [column for column in group_by if column not in df.columns]
    if missing_group_cols:
        raise ValueError(f"Missing group_by columns: {', '.join(missing_group_cols)}")

    grouped = df.groupby(group_by, dropna=False)
    result = grouped.size().reset_index(name="__row_count")

    for aggregation in aggregations:
        header = aggregation["header"]
        function = aggregation["function"]

        if function == "count":
            result[header] = result["__row_count"]
            continue

        source_column = aggregation.get("source_column", header)
        if source_column not in df.columns:
            raise ValueError(f"Missing aggregate source column: {source_column}")

        if function == "sum":
            values = df.copy()
            values[source_column] = pd.to_numeric(values[source_column], errors="coerce").fillna(0)
            summed = (
                values.groupby(group_by, dropna=False)[source_column]
                .sum()
                .reset_index(name=header)
            )
            result = result.merge(summed, on=group_by, how="left")
            continue

        raise ValueError(f"Unsupported aggregate function: {function}")

    result = result.drop(columns=["__row_count"])
    ordered_columns = [column for column in output_order if column in result.columns]
    remaining_columns = [column for column in result.columns if column not in ordered_columns]
    return result[ordered_columns + remaining_columns]


def write_to_sheets(df, sheet_tab):
    gc = get_sheets_client()
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = spreadsheet.worksheet(sheet_tab)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=sheet_tab,
            rows=max(len(df) + 10, 1000),
            cols=max(len(df.columns) + 5, 25),
        )

    ws.update(
        [df.columns.values.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED",
    )
    ws.format("1:1", {"textFormat": {"bold": True}})

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.update_note("A1", f"Last synced: {run_time}")

    log.info("Data written to Google Sheets tab: %s", sheet_tab)


def write_sections_to_sheets(sections, sheet_tab):
    gc = get_sheets_client()
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = spreadsheet.worksheet(sheet_tab)
        ws.clear()
    except gspread.WorksheetNotFound:
        total_rows = sum(len(df) + 4 for _, df in sections)
        max_cols = max((len(df.columns) for _, df in sections), default=1)
        ws = spreadsheet.add_worksheet(
            title=sheet_tab,
            rows=max(total_rows + 10, 1000),
            cols=max(max_cols + 5, 25),
        )

    values = []
    for job_name, df in sections:
        values.append([job_name])
        values.append(df.columns.values.tolist())
        values.extend(df.values.tolist())
        values.append([])

    ws.update(values, value_input_option="USER_ENTERED")

    row = 1
    for _, df in sections:
        ws.format(f"{row}:{row + 1}", {"textFormat": {"bold": True}})
        row += len(df) + 4

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.update_note("A1", f"Last synced: {run_time}")

    log.info("Data written to combined Google Sheets tab: %s", sheet_tab)


def run_job(client, config):
    job_name = config.get("job_name", "HubSpot to Sheets ETL")
    log.info("%s started", job_name)

    pipeline_map, stage_map = get_pipeline_stage_maps(client, config["primary_object"])
    owner_map = get_owner_map(client)
    primary_records = fetch_primary_objects(client, config, stage_map)

    if not primary_records:
        log.warning("No records found")
        return

    df = transform_rows(primary_records, client, config, pipeline_map, stage_map, owner_map)

    if df.empty:
        log.warning("No rows created")
        return None

    log.info("Final dataframe shape: %s", df.shape)
    log.info("%s complete", job_name)
    return job_name, df


def main():
    validate_env()
    config = load_config()
    jobs = config.get("jobs", [config])
    client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
    combined_sheet_tab = config.get("google_sheet_tab")
    sections = []

    for job in jobs:
        result = run_job(client, job)
        if not result:
            continue

        if combined_sheet_tab:
            sections.append(result)
        else:
            job_name, df = result
            write_to_sheets(df, job.get("google_sheet_tab", job_name))

    if combined_sheet_tab and sections:
        write_sections_to_sheets(sections, combined_sheet_tab)


if __name__ == "__main__":
    main()
