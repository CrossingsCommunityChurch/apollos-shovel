from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id

from html_sanitizer import Sanitizer
import nltk

import requests

nltk.download('punkt')

def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
        except TypeError:
            return None
    return dct


summary_sanitizer = Sanitizer({
    'tags': {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'},
    'empty': {},
    'separate': {},
    'attributes': {},
})

html_allowed_tags = {
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'blockquote',
    'p',
    'a',
    'ul',
    'ol',
    'li',
    'b',
    'i',
    'strong',
    'em',
    'br',
    'caption',
    'img',
    'div',
}


html_sanitizer = Sanitizer({
    'tags': html_allowed_tags,
    'empty': {},
    'seperate': {},
    'attributes': {
        **{
            'a': {'href', 'target', 'rel'},
            'img': {'src'},
        },
        **dict.fromkeys(html_allowed_tags, {'class', 'style'})
    }
})

def create_summary(item):
    summary_value = safeget(item, 'AttributeValues', 'Attribute', 'Value')
    if summary_value and summary_value is not '':
        return summary_value

    if not item['Content']:
        return ''

    cleaned = summary_sanitizer.sanitize(item['Content'])
    sentences = nltk.sent_tokenize(cleaned)
    return sentences[0]

def create_html_content(item):
    return html_sanitizer.sanitize(item['Content'])


def fetch_and_save_content_items(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            # "$expand": "Photo",
            # "$select": "Id,Content",
            "loadAttributes": "simple",
            "attributeKeys": "Summary",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=headers)
        rock_objects = r.json()

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")
            print("top: {top}")
            print("skip: {skip}")
            skip += top
            continue


        skip += top
        fetched_all = len(rock_objects) < top

        def photo_url(path):
            if path is None:
                return None
            elif path.startswith("~"):
                rock_host = (Variable.get(kwargs['client'] + '_rock_api')).split("/api")[0]
                return path.replace("~", rock_host)
            else:
                return path



        # "createdAt","updatedAt", "originId", "originType", "apollosType", "summary", "htmlContent", "title", "publishAt"
        def update_content(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'UniversalContentItem',
                create_summary(obj),
                create_html_content(obj),
                obj['Title'],
                obj['StartDateTime'],
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(map(fix_casing, ("createdAt", "updatedAt", "originId", "originType", "apollosType", "summary", "htmlContent", "title", 'publishAt')))


        pg_hook.insert_rows(
            '"contentItems"',
            content_to_insert,
            columns,
            0,
            True,
            replace_index = ('"originId"', '"originType"')
        )


        add_apollos_ids = """
        UPDATE "contentItems"
        SET "apollosId" = "apollosType" || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)

