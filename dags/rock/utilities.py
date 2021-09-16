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
        except IndexError:
            return None
    return dct


def find_supported_fields(pg_hook, insert_data, table_name):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Get a list of columns by table name.
    cursor.execute(f"select * from {table_name} limit 0;")
    colnames = set([desc[0] for desc in cursor.description])
    conn.close()
    cursor.close()

    if insert_data:
        print("Omitting the following columns")
        print(set(insert_data[0].keys()).difference(colnames))

    # Find the columns that are not in the list of columns in the database.
    data_with_valid_keys = map(
        lambda insert_row: {
            k: insert_row[k]
            for k in insert_row.keys()
            if k not in list(set(insert_row.keys()).difference(colnames))
        },
        insert_data,
    )

    # Sort the resulting dictonary by it's keys
    # We need the insert array to match the list of columns to insert
    sorted_insert_data = list(
        map(lambda r: {key: r[key] for key in sorted(r.keys())}, data_with_valid_keys)
    )

    # Now we turm our dictonary into our list.
    insert_data_as_list = [row.values() for row in sorted_insert_data]

    # Andddd, here's where we set the column names that we will be inserting.
    # By default, using the colnames array.
    col_names_to_insert = colnames
    # Identify the data intersection using the first row
    # This is to catch situations where we have more columns in our database
    # than columns in our insert
    if sorted_insert_data:
        col_names_to_insert = sorted_insert_data[0].keys()

    # Return the data to insert and our (sorted) list.
    return (insert_data_as_list, sorted(list(col_names_to_insert)))


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
