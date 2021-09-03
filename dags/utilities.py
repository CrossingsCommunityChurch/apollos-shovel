import pytz
from airflow.models import Variable
from dateutil import parser
import requests


def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
        except TypeError:
            return None
    return dct


def safeget_no_case(dct, *keys):
    dct = {k.lower(): v for k, v in dct.items()}
    for key in keys:
        try:
            dct = dct[key.lower()]
            if type(dct) is dict:
                dct = {k.lower(): v for k, v in dct.items()}
        except KeyError:
            return None
        except TypeError:
            return None
    return dct


def get_modified_item_ids_from_attrs(kwargs):
    # Get the ItemEntityID. We'll use it to find the corect update attribute values.
    item_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
        headers={"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")},
    ).json()[0]["Id"]

    # Get all attribute values that were created
    entity_id_result = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/AttributeValues",
        params={
            "$filter": f"({get_delta_offset(kwargs)}) and Attribute/EntityTypeId eq {item_entity_id}"
        },
        headers={"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")},
    ).json()

    entity_ids = set([a["EntityId"] for a in entity_id_result])

    # We can't fetch more than 16 items at once due to a rock limitation.
    # These items will need to be manually touched, or grabbed during a periodic backfill.
    if len(entity_ids) > 16:
        return ""

    # Turn the IDS into a filter that resembles SQLs `id in (...)`
    return " or ".join([f"Id eq {eid}" for eid in entity_ids])


def rock_timestamp_to_utc(timestamp, kwargs):
    # Get the local timezone
    local_zone = pytz.timezone(
        Variable.get(kwargs["client"] + "_rock_tz", default_var="EST")
    )
    # Find the UTC offset, for that time and timezone. it will be different depending on DST.
    offset_hours = (
        local_zone.localize(parser.parse(timestamp)).utcoffset().total_seconds()
        / 60
        / 60
    )
    # Return that original date, this time with the correct offset appended.
    return parser.parse(f"{timestamp}{int(offset_hours)}")


def get_delta_offset_with_content_attributes(kwargs):
    attr_value_filter = get_modified_item_ids_from_attrs(kwargs)
    # If the filter for fetching specific items contains any items
    if attr_value_filter:
        # Use the filter
        return f"({get_delta_offset(kwargs)}) or ({attr_value_filter})"
    # Otherwise use the normal filter.
    return get_delta_offset(kwargs)


def get_delta_offset(kwargs):
    local_zone = pytz.timezone(
        Variable.get(kwargs["client"] + "_rock_tz", default_var="EST")
    )
    execution_date_string = (
        kwargs["execution_date"].astimezone(local_zone).strftime("%Y-%m-%dT%H:%M:%S")
    )
    print(
        f"ModifiedDateTime ge datetime'{execution_date_string}' or (ModifiedDateTime eq null and CreatedDateTime ge datetime'{execution_date_string}')"
    )
    return f"ModifiedDateTime ge datetime'{execution_date_string}' or (ModifiedDateTime eq null and CreatedDateTime ge datetime'{execution_date_string}')"
