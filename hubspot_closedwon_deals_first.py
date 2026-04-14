import json
import logging
import os
from datetime import datetime, timezone

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
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "HubSpot Closed Won Deals")

CONTACT_PROPERTIES = [
    "createdate",
    "firstname",
    "lastname",
    "your_business_is_making_",
    "utm_source",
    "company",
]

DEAL_PROPERTIES = [
    "dealname",
    "pipeline",
    "dealstage",
    "amount",
    "closedate",
    "hubspot_owner_id",
    "secondary_deal_owner",
    "tertiary_deal_owner",
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PAGE_SIZE = 100
CLOSED_WON_LABEL = "Closed Won"


def validate_env():
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("Missing HUBSPOT_ACCESS_TOKEN")
    if not GOOGLE_SA_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    if not GOOGLE_SHEET_ID:
        raise ValueError("Missing GOOGLE_SHEET_ID")

    log.info("Environment variables loaded")


def get_month_range_ms():
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    end = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def get_sheets_client():
    if not GOOGLE_SA_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is missing")

    try:
        info = json.loads(GOOGLE_SA_JSON)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception:
        pass

    if not os.path.isfile(GOOGLE_SA_JSON):
        raise FileNotFoundError(f"JSON file not found: {GOOGLE_SA_JSON}")

    creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=SCOPES)
    return gspread.authorize(creds)


def get_pipeline_stage_maps(client):
    pipeline_map = {}
    stage_map = {}

    pipelines = client.crm.pipelines.pipelines_api.get_all(object_type="deals")

    for pipeline in pipelines.results:
        pipeline_map[pipeline.id] = pipeline.label
        for stage in pipeline.stages:
            stage_map[stage.id] = stage.label

    return pipeline_map, stage_map


def get_closed_won_stage_ids(stage_map):
    return [stage_id for stage_id, label in stage_map.items() if label == CLOSED_WON_LABEL]


def get_owner_map(client):
    owner_map = {}
    after = None

    while True:
        response = client.crm.owners.owners_api.get_page(limit=100, after=after)

        for owner in response.results:
            first_name = getattr(owner, "first_name", "") or ""
            last_name = getattr(owner, "last_name", "") or ""
            full_name = f"{first_name} {last_name}".strip()

            if not full_name:
                full_name = getattr(owner, "email", "") or str(owner.id)

            owner_map[str(owner.id)] = full_name

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if not after:
            break

    return owner_map


def fetch_closed_won_deals_for_month(client, start_ms, end_ms, closed_won_stage_ids):
    if not closed_won_stage_ids:
        log.warning("No Closed Won stage IDs found")
        return []

    all_deals = []
    after = None

    log.info("Fetching Closed Won deals for this month")

    while True:
        request = PublicObjectSearchRequest(
            filter_groups=[
                FilterGroup(
                    filters=[
                        Filter(
                            property_name="closedate",
                            operator="BETWEEN",
                            value=str(start_ms),
                            high_value=str(end_ms),
                        ),
                        Filter(
                            property_name="dealstage",
                            operator="IN",
                            values=closed_won_stage_ids,
                        ),
                    ]
                )
            ],
            properties=DEAL_PROPERTIES,
            limit=PAGE_SIZE,
            after=after,
        )

        response = client.crm.deals.search_api.do_search(
            public_object_search_request=request
        )

        all_deals.extend(response.results)

        log.info("Fetched %d deals (total: %d)", len(response.results), len(all_deals))

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if not after:
            break

    return all_deals


def get_associated_contact_ids(client, deal_id):
    try:
        response = client.crm.associations.v4.basic_api.get_page(
            object_type="deals",
            object_id=str(deal_id),
            to_object_type="contacts",
            limit=100,
        )

        contact_ids = []

        for item in response.results:
            if hasattr(item, "to_object_id"):
                contact_ids.append(str(item.to_object_id))
            elif hasattr(item, "id"):
                contact_ids.append(str(item.id))

        return contact_ids

    except Exception as exc:
        log.warning("Could not fetch associated contacts for deal %s: %s", deal_id, exc)
        return []


def get_contacts_by_ids(client, contact_ids):
    unique_ids = list({str(cid) for cid in contact_ids if cid})
    if not unique_ids:
        return {}

    contacts_map = {}
    chunk_size = 100

    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i:i + chunk_size]

        try:
            response = client.crm.contacts.batch_api.read(
                batch_read_input_simple_public_object_id={
                    "properties": CONTACT_PROPERTIES,
                    "inputs": [{"id": cid} for cid in chunk],
                }
            )

            for result in response.results:
                contacts_map[str(result.id)] = result.properties or {}

        except Exception as exc:
            log.warning("Could not batch fetch contacts: %s", exc)

    return contacts_map


def format_datetime_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = (
            pd.to_datetime(df[column_name], errors="coerce", utc=True)
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )


def transform_rows(deals, client, pipeline_map, stage_map, owner_map):
    rows = []
    deal_to_contact_ids = {}
    all_contact_ids = []

    log.info("Fetching associated contacts for deals")

    for idx, deal in enumerate(deals, start=1):
        deal_id = str(deal.id)
        associated_contact_ids = get_associated_contact_ids(client, deal_id)
        deal_to_contact_ids[deal_id] = associated_contact_ids
        all_contact_ids.extend(associated_contact_ids)

        if idx % 10 == 0:
            log.info("Processed associations for %d / %d deals", idx, len(deals))

    log.info("Batch fetching %d unique contacts", len(set(all_contact_ids)))
    contacts_map = get_contacts_by_ids(client, all_contact_ids)

    for deal in deals:
        deal_id = str(deal.id)
        deal_props = deal.properties or {}
        associated_contact_ids = deal_to_contact_ids.get(deal_id, [])

        pipeline_label = pipeline_map.get(
            deal_props.get("pipeline", ""),
            deal_props.get("pipeline", ""),
        )
        stage_label = stage_map.get(
            deal_props.get("dealstage", ""),
            deal_props.get("dealstage", ""),
        )

        primary_owner = owner_map.get(
            str(deal_props.get("hubspot_owner_id", "") or ""),
            str(deal_props.get("hubspot_owner_id", "") or ""),
        )
        secondary_owner = owner_map.get(
            str(deal_props.get("secondary_deal_owner", "") or ""),
            str(deal_props.get("secondary_deal_owner", "") or ""),
        )
        tertiary_owner = owner_map.get(
            str(deal_props.get("tertiary_deal_owner", "") or ""),
            str(deal_props.get("tertiary_deal_owner", "") or ""),
        )

        if not associated_contact_ids:
            rows.append({
                "contact_createdate": "",
                "firstname": "",
                "lastname": "",
                "your_business_is_making_": "",
                "utm_source": "",
                "company": "",
                "dealname": deal_props.get("dealname", ""),
                "pipeline": pipeline_label,
                "dealstage": stage_label,
                "amount": deal_props.get("amount", ""),
                "closedate": deal_props.get("closedate", ""),
                "deal_owner": primary_owner,
                "secondary_deal_owner": secondary_owner,
                "tertiary_deal_owner": tertiary_owner,
            })
            continue

        for contact_id in associated_contact_ids:
            contact_props = contacts_map.get(str(contact_id), {})

            rows.append({
                "contact_createdate": contact_props.get("createdate", ""),
                "firstname": contact_props.get("firstname", ""),
                "lastname": contact_props.get("lastname", ""),
                "your_business_is_making_": contact_props.get("your_business_is_making_", ""),
                "utm_source": contact_props.get("utm_source", ""),
                "company": contact_props.get("company", ""),
                "dealname": deal_props.get("dealname", ""),
                "pipeline": pipeline_label,
                "dealstage": stage_label,
                "amount": deal_props.get("amount", ""),
                "closedate": deal_props.get("closedate", ""),
                "deal_owner": primary_owner,
                "secondary_deal_owner": secondary_owner,
                "tertiary_deal_owner": tertiary_owner,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    format_datetime_column(df, "contact_createdate")
    format_datetime_column(df, "closedate")

    ordered_cols = [
        "contact_createdate",
        "firstname",
        "lastname",
        "your_business_is_making_",
        "utm_source",
        "company",
        "dealname",
        "pipeline",
        "dealstage",
        "amount",
        "closedate",
        "deal_owner",
        "secondary_deal_owner",
        "tertiary_deal_owner",
    ]

    existing_cols = [col for col in ordered_cols if col in df.columns]
    df = df[existing_cols]
    df = df.fillna("").astype(str)

    return df


def write_to_sheets(df):
    gc = get_sheets_client()
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = spreadsheet.worksheet(GOOGLE_SHEET_TAB)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=GOOGLE_SHEET_TAB,
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

    log.info("Data written to Google Sheets")


def main():
    log.info("Closed Won deals-first ETL started")

    validate_env()

    start_ms, end_ms = get_month_range_ms()

    client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)

    pipeline_map, stage_map = get_pipeline_stage_maps(client)
    owner_map = get_owner_map(client)
    closed_won_stage_ids = get_closed_won_stage_ids(stage_map)

    deals = fetch_closed_won_deals_for_month(client, start_ms, end_ms, closed_won_stage_ids)

    if not deals:
        log.warning("No Closed Won deals found for this month")
        return

    df = transform_rows(deals, client, pipeline_map, stage_map, owner_map)

    if df.empty:
        log.warning("No rows created from Closed Won deals")
        return

    log.info("Final dataframe shape: %s", df.shape)

    write_to_sheets(df)

    log.info("Closed Won deals-first ETL complete")


if __name__ == "__main__":
    main()
