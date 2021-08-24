from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests
from rock.rock_media import is_media_image
from utilities import get_delta_offset


class CoverImage:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.headers = {
            "Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")
        }

        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def update_content_item_cover_image(self, args):
        contentItemId = str(args["ContentItemId"])
        coverImageId = str(args["CoverImageId"])

        return self.pg_hook.run(
            "UPDATE content_item SET cover_image_id = %s WHERE origin_id::Integer = %s",
            True,
            (coverImageId, contentItemId),
        )

    def get_best_image_id(self, images, content_item):
        imageId = None
        if len(images) > 1:
            squareImages = list(
                filter(lambda attribute: "square" in attribute["Key"].lower(), images)
            )
            if len(squareImages) > 0:
                imageId = squareImages[0]["Id"]
            else:
                imageId = images[0]["Id"]
        elif len(images) == 1:
            imageId = images[0]["Id"]

        if imageId:
            concatImageId = str(content_item["Id"]) + "/" + str(imageId)
            try:
                return self.pg_hook.get_first(
                    "SELECT id FROM media WHERE origin_id = %s", (concatImageId,)
                )[0]
            except:  # noqa E722
                print("Did not find media we were expecting")
                print(
                    f"Looking for media with ID {str(content_item['Id'])  + '/' + str(imageId)}"
                )
                return None

        return None

    def get_channel_image(self, content_item):
        rock_channel_id = content_item["ContentChannelId"]
        images = self.pg_hook.get_first(
            "select media.id from content_item_category as c inner join media on media.node_id = c.id and media.node_type = 'ContentItemCategory' where c.origin_id = %s and c.origin_type = 'rock' ;",
            (f"{rock_channel_id}",),
        )

        return images[0] if images else None

    def is_image(self, content_item, attribute):
        return is_media_image(content_item, attribute)

    def map_content_items(self, content_item):
        image_attributes = list(
            filter(
                lambda a: self.is_image(content_item, a),
                content_item["Attributes"].values(),
            )
        )

        cover_image_id = self.get_best_image_id(image_attributes, content_item)

        if cover_image_id:
            self.update_content_item_cover_image(
                {"ContentItemId": content_item["Id"], "CoverImageId": cover_image_id}
            )
        else:
            channel_image_id = self.get_channel_image(content_item)
            if channel_image_id:
                self.update_content_item_cover_image(
                    {
                        "ContentItemId": content_item["Id"],
                        "CoverImageId": channel_image_id,
                    }
                )

    def run_fetch_and_save_cover_image(self):
        fetched_all = False
        skip = 0
        top = 1000

        while not fetched_all:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                # "$select": "Id,Content",
                "loadAttributes": "expanded",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset(self.kwargs)

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                skip += top
                continue

            # Sets all content items without a cover image to their parent's cover image

            skip += top
            fetched_all = len(rock_objects) < top

        # Sets all content items without a cover image to their parent's cover image
        self.pg_hook.run(
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


def fetch_and_save_cover_image(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = CoverImage if "klass" not in kwargs else kwargs["klass"]

    cover_image_task = Klass(kwargs)

    cover_image_task.run_fetch_and_save_cover_image()
