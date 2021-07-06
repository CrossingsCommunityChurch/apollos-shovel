import sys
import os

# Give this test access to the dag files.
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from rock_content_items import get_typename


config = {
  "CONTENT_MAPPINGS": {
    "ContentSeriesContentItem": {
      "ContentChannelTypeId": [1]
    }
  }
}

item = {
  "ContentChannelTypeId": 1,
  "Attributes": {}
}

print(get_typename(item, config))

config = {
  "CONTENT_MAPPINGS": {
    "ContentSeriesContentItem": {
      "ContentChannelTypeId": [1]
    }
  }
}

item = {
  "ContentChannelTypeId": 2,
  "Attributes": {}
}

print(get_typename(item, config))

config = {
  "CONTENT_MAPPINGS": {
    "ContentSeriesContentItem": {
      "ContentChannelTypeId": [1]
    }
  }
}

item = {
  "Attributes": {
    "Video": {
      "FieldTypeId": 79,
      "Key": "Video"
    }
  },
  "AttributeValues": {
    "Video": {
      "Value": "http://apollos.app/video.mp4"
    }
  }
}

print(get_typename(item, config))