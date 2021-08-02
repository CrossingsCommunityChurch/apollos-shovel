from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests

def get_images (content_item):
    def filter_images( attribute ):
            attributeKey = attribute['Key']
            attributeValue = content_item['AttributeValues'][attributeKey]['Value']
          
            return (attribute['FieldTypeId'] == 10 and attributeValue) or ('image' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http'))

    return list(filter(filter_images, content_item['Attributes'].values()))

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

    def update_content_item_cover_image(args):
        contentItemId =str(args['ContentItemId'])
        coverImageId = str(args['CoverImageId'])

        return pg_hook.run(
            'UPDATE content_item SET cover_image_id = %s WHERE origin_id::Integer = %s', 
            True, 
            (coverImageId, contentItemId))

    def map_content_items(content_item):

        def get_best_image_id (images):
            imageId = None;
            if(len(images) > 1):
                squareImages = list(filter(lambda attribute: 'square' in attribute['Key'].lower(), images ))
                if(len(squareImages) > 0):
                    imageId = squareImages[0]['Id']
                else:
                    imageId = images[0]['Id']
            elif(len(images) == 1):
                imageId = images[0]['Id']

            if(imageId):
                concatImageId = str(content_item['Id'])  + '/' + str(imageId)
                return pg_hook.get_first('SELECT id FROM media WHERE origin_id = %s', (concatImageId,))[0]
            
            return None

        imageAttributes = get_images(content_item)
        coverImageId = get_best_image_id(imageAttributes)

        if(coverImageId):
            update_content_item_cover_image({
                "ContentItemId": content_item['Id'],
                "CoverImageId": coverImageId
            })

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
                
        contentItems = list(map(map_content_items, r.json()))

        #Sets all content items without a cover image to their parent's cover image
        pg_hook.run(
            """
                UPDATE content_item
                SET    cover_image_id = r.cover_image_id
                FROM   (
                    SELECT content_item.id AS childId, parent.cover_image_id
                    FROM    content_item
                    INNER JOIN content_item_connection ON content_item_connection.child_id = content_item.id
                    INNER JOIN content_item AS parent ON content_item_connection.parent_id = parent.id
                    WHERE  content_item.cover_image_id IS NULL
                ) AS r
                WHERE  r.childId = content_item.id;
            """
        )

        skip += top
        fetched_all = len(r.json()) < top
