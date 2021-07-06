from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


def is_media_image(attribute, contentItem):
    attributeKey = attribute['Key']
    attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
    return attribute['FieldTypeId'] == 10 or ('image' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http'))

def is_media_video(attribute, contentItem):
    attributeKey = attribute['Key']
    attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
    return [79, 80].count(attribute['FieldTypeId']) == 1 or 'video' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http')

def is_media_audio(attribute, contentItem):
    attributeKey = attribute['Key']
    attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
    return [77, 78].count(attribute['FieldTypeId']) == 1 or 'audio' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http')


def fetch_and_save_media(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    def get_content_item_id(rockOriginId):
        return pg_hook.get_first(f'SELECT id FROM "contentItems" WHERE "originId"::Integer = {rockOriginId}')[0]

    def mapContentItems(contentItem):

        nodeId = get_content_item_id(contentItem['Id'])

        def filter_media_attributes( attribute ):
            return is_media_image(attribute, contentItem) or is_media_video(attribute, contentItem) or is_media_audio (attribute, contentItem)

        def get_media_type( attribute ):
            if(is_media_image(attribute, contentItem)):
                return 'IMAGE'
            elif(is_media_video(attribute, contentItem)):
                return 'VIDEO'
            elif(is_media_audio(attribute, contentItem)):
                return 'AUDIO'
            else:
                return 'UNKNOWN_MEDIA_TYPE'

        def get_media_value( attribute  ):
            mediaType = get_media_type ( attribute )
            attributeKey = attribute['Key']
            attributeValue = contentItem['AttributeValues'][attributeKey]['Value']

            if(mediaType == 'IMAGE'):
                return "https://rock.apollos.app/GetImage.ashx?guid=" + attributeValue if len(attributeValue) > 0 else ''
            else:
                return attributeValue

        def map_attributes( attribute ):
            attributeKey = attribute['Key']
            attributeFieldType = attribute['FieldTypeId']
            attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
            attributeValueId = str(contentItem['Id']) + "/" + str(attribute['Id'])
            mediaType = get_media_type( attribute )
            mediaValue = get_media_value( attribute )

            if(mediaValue):
                return (
                    'Media',
                    kwargs['execution_date'],
                    kwargs['execution_date'],
                    nodeId,
                    'ContentItem',
                    mediaType,
                    mediaValue,
                    attributeValueId,
                    'rock'
                )

            return None


        filteredAttributes = filter(filter_media_attributes, contentItem['Attributes'].values())
        mappedAttributes = map(map_attributes, filteredAttributes)

        return list(filter(lambda media: bool(media), mappedAttributes))

    fetched_all = False
    skip = 0
    top = 10000

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            # "$select": "Id,Content",
            "loadAttributes": "expanded",
            "$orderby": "ModifiedDateTime desc",
        }
        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=headers)

        def fix_casing(col):
            return "\"{}\"".format(col)

        mediaAttributeLists = list(map(mapContentItems, r.json()))
        mediaAttributes = [mediaAttribute for sublist in mediaAttributeLists for mediaAttribute in sublist]
        columns = list(map(fix_casing, ('apollosType', 'createdAt', 'updatedAt', 'nodeId', 'nodeType', 'type', 'url', 'originId', 'originType')))

        print('Media Items Aded: ')
        print(len(list(mediaAttributes)))

        pg_hook.insert_rows(
            '"media"',
            list(mediaAttributes),
            columns,
            0,
            True,
            replace_index = ('"originId"', '"originType"')
        )

        add_apollos_ids = """
        UPDATE "media"
        SET "apollosId" = "apollosType" || ':' || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)

        skip += top
        fetched_all = len(r.json()) < top