from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests

def get_images (content_item):
    def filter_images( attribute ):
            attributeKey = attribute['Key']
            attributeValue = content_item['AttributeValues'][attributeKey]['Value']

            return (attribute['FieldTypeId'] == 10 and attributeValue is not "") or ('image' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http'))

    return list(filter(filter_images, content_item['Attributes'].values()))

def get_best_image_id (images):
    if(len(images) > 1):
        squareImages = list(filter(lambda attribute: 'square' in attribute['Key'].lower(), images ))
        if(len(squareImages) > 0):
            return squareImages[0]['Guid']
        else:
            return images[0]['Guid']
    elif(len(images) == 1):
        return images[0]['Guid']
    else:
        return None


def fetch_and_save_cover_image(ds, *args, **kwargs):
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

    fetched_all = False
    skip = 0
    top = 10000

    def map_content_items(content_item):

        imageAttributes = get_images(content_item)
        coverImageId = get_best_image_id(imageAttributes)

        if(coverImageId):
            return {
                "ContentItemId": content_item['Id'],
                "CoverImageId": coverImageId
            }
        '''         
        else:
            contentItemId = content_item['Id']

            parents = requests.get(
                    f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItemAssociations",
                    params={
                        "$filter": f'ChildContentChannelItemId eq {contentItemId}',
                    },
                    headers=headers).json()

            if(len(parents) == 1):
                parentContentItemId = parents[0]['ContentChannelItemId']
                parentContentItem = requests.get(
                    f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems/{parentContentItemId}",
                    params={"loadAttributes": "expanded"},
                    headers=headers).json()
                coverImageId = get_best_image_id(get_images(parentContentItem))
                
                return {
                    "ContentItemId": content_item['Id'],
                    "CoverImageId": coverImageId
                } '''


    def update_content_item_cover_image(args):
        if(args):
            contentItemId = args['ContentItemId']
            coverImageId = args['CoverImageId']
            return pg_hook.run(
                'UPDATE "contentItems" SET "coverImageId" = (SELECT id FROM "media" WHERE "originId" = %s) WHERE "originId"::Integer = %s', 
                True, 
                (coverImageId, contentItemId))

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
                
        contentItems = list(map(map_content_items, r.json()))
        updatedItems = list(map(update_content_item_cover_image, contentItems))

        skip += top
        fetched_all = len(r.json()) < top
