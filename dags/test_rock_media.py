import requests
import json
import psycopg2


pg_hook = psycopg2.connect(
    host="192.168.1.110",
    database="postgres",
    user="thomasthornton",
)

def get_content_item_id(rockOriginId):
    content_item_cursor = pg_hook.cursor()
    content_item_cursor.execute(f'SELECT id FROM "contentItems" WHERE "originId"::Integer = {rockOriginId}')
    content_item_id = content_item_cursor.fetchone()[0]
    content_item_cursor.close()
    return content_item_id


def mapContentItems(contentItem):

        nodeId = get_content_item_id(contentItem['Id'])

        def is_media_image( attribute ):
            attributeKey = attribute['Key']
            attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
            return attribute['FieldTypeId'] == 10 or ('image' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http'))
            
        def is_media_video ( attribute ):
            attributeKey = attribute['Key']
            attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
            return [79, 80].count(attribute['FieldTypeId']) == 1 or 'video' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http')

        def is_media_audio ( attribute ):
            attributeKey = attribute['Key']
            attributeValue = contentItem['AttributeValues'][attributeKey]['Value']
            return [77, 78].count(attribute['FieldTypeId']) == 1 or 'audio' in attributeKey.lower() and isinstance(attributeValue, str) and attributeValue.startswith('http')

        


        def filter_media_attributes( attribute ):
            return is_media_image( attribute) or is_media_video( attribute ) or is_media_audio ( attribute )

        def get_media_type( attribute ):
            if(is_media_image( attribute )):
                return 'IMAGE'
            elif(is_media_video( attribute )):
                return 'VIDEO'
            elif(is_media_audio( attribute )):
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

            return {
                "type": get_media_type( attribute ),
                "nodeId": nodeId,
                "nodeType": 'ContentItem',
                "url": get_media_value( attribute ),
                "attribute": attribute,
                "attributeValue": contentItem['AttributeValues'][attributeKey]

            }

        filteredAttributes = filter(filter_media_attributes, contentItem['Attributes'].values())
        mappedAttributes = map(map_attributes, filteredAttributes)

        return list(mappedAttributes)

def fetch_and_save_media():
    headers = {"Authorization-Token": "ASZjZWdf3IqrbZX9sedtB4wb"}

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
                "https://rock.apollos.app/api/ContentChannelItems",
                params=params,
                headers=headers)
                
        mediaAttributeLists = list(map(mapContentItems, r.json()))
        mediaAttributes = [mediaAttribute for sublist in mediaAttributeLists for mediaAttribute in sublist]

        skip += top
        fetched_all = len(r.json()) < top


fetch_and_save_media()
pg_hook.close()
