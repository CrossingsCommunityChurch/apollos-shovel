import re
from contentful_crossroads.cr_contentful_content_items import ContentItem


accepted_distribution_channels = ["www.crossroads.net"]
kids_club_category_id = "24jij4G5LUqBaM0c9jBsuW"


class KidsClubContentItem(ContentItem):
    def __init__(self, kwargs):
        self.kwargs = kwargs
        super(KidsClubContentItem, self).__init__(kwargs)

    def get_video_url(self, entry):
        # Getting the video url is a big ol' TBD for Kids' Club.
        # We don't have streaming links for the video files yet.
        # In the meantime, we'll use a placeholder, the same for all videos.
        return "https://player.vimeo.com/external/522397154.m3u8?s=288eb6e0d740e0b2344b7d003b59f1b4245dfde3"

    def check_distribution_channels(self, entry):
        try:
            distribution_channels = list(
                map(lambda item: item["site"], entry.distribution_channels)
            )
            return any(
                item in distribution_channels for item in accepted_distribution_channels
            )
        except AttributeError:
            return False

    def check_is_kids_club(self, entry):
        try:
            category_id = entry.category.id
            return category_id == kids_club_category_id
        except AttributeError:
            return False

    def get_items_to_save(self, items):
        def should_save_item(entry):
            distribution_channel_check = self.check_distribution_channels(entry)
            if not distribution_channel_check:
                return False
            is_kids_club = self.check_is_kids_club(entry)
            if not is_kids_club:
                return False
            return distribution_channel_check and is_kids_club

        return list(filter(should_save_item, items))

    def get_category_id(self, entry):
        # This is NOT the right way to do this. Since the older/younger categories
        #   don't exist in contentful, the only way to identify what category a video
        #   belongs in is by parsing the title. Better than nothing.
        title = entry.title
        younger_match = re.search(r"younger", title, re.IGNORECASE)
        pg_entry_id = None
        if younger_match:
            pg_entry_id = self.pg_hook.get_first(
                "SELECT id FROM content_item_category WHERE origin_id = 'younger-kids'"
            )[0]
        else:
            pg_entry_id = self.pg_hook.get_first(
                "SELECT id FROM content_item_category WHERE origin_id = 'older-kids'"
            )[0]
        return pg_entry_id

    def get_title(self, entry):
        title = entry.title
        split_title = title.split("(")
        return split_title[0].strip()
