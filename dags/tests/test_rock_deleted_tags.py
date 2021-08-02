import requests
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

API_BASE_URL = os.getenv('api-base-url')
API_TOKEN= os.getenv('')


pg_hook = psycopg2.connect(
    host="192.168.1.113",
    database="postgres",
    user="thomasthornton",
)

pg_cursor = pg_hook.cursor()

def delete_tags(tags):
    pg_cursor.execute("""
        DELETE FROM "tags"
        WHERE id = ANY(%s::uuid[])
    """, (tags,))


def get_deleted_tags():
    headers = {"Authorization-Token": "ASZjZWdf3IqrbZX9sedtB4wb"}

    pg_cursor.execute('SELECT id, "originId" FROM "tags"')

    postgresTags = list(pg_cursor.fetchall());

    person_entity_id = requests.get(
        "https://rock.apollos.app/api/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers
    ).json()[0]['Id']

    params = {
        "$filter": f"EntityTypeId eq {person_entity_id} and CategoryId eq {186}",
        "$select": "Id",
        "$orderby": "ModifiedDateTime desc",
    }

    r = requests.get(
            "https://rock.apollos.app/api/DataViews",
            params=params,
            headers=headers)
            
    rockTags = list(map(lambda tag: tag['Id'], r.json()));
    
    deletedTags = list(
        map(
            lambda tag: tag[0],
            filter(
                lambda tag: int(tag[1]) not in rockTags,
                postgresTags
            )
        )
    );

    delete_tags(deletedTags)
    print('hello')
    

get_deleted_tags()
pg_hook.commit()
pg_cursor.close()
pg_hook.close()
