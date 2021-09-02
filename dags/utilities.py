import pytz
from airflow.models import Variable
from dateutil import parser


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
