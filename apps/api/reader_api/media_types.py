from __future__ import annotations

SOURCE_MEDIA_TYPES = (
    "article",
    "social",
    "image",
    "video",
    "podcast",
    "notification",
)

# Compatibility is only for old external OPML and the 0069 pre-migration audit.
LEGACY_MEDIA_FOLDER_NAMES = {
    "social": {"social", "socialmedia", "social media"},
    "image": {"image", "images", "picture", "pictures", "photo", "photos"},
    "video": {"video", "videos"},
    "podcast": {"audio", "audios", "podcast", "podcasts"},
    "notification": {"notification", "notifications"},
}


def media_type_from_legacy_folder_path(path: list[str]) -> str:
    parts = {part.strip().lower() for part in path}
    for media_type, names in LEGACY_MEDIA_FOLDER_NAMES.items():
        if parts & names:
            return media_type
    return "article"


def normalize_folder_name(name: str, media_type: str) -> str:
    value = name.strip()
    prefix, separator, rest = value.partition("/")
    if media_type == "article" and separator and prefix.strip().casefold() == "articles":
        return rest.strip() or value
    return value


def effective_legacy_source_media_type(source_media_type: str, folder_name: str) -> str:
    if source_media_type in SOURCE_MEDIA_TYPES and source_media_type != "article":
        return source_media_type
    return media_type_from_legacy_folder_path(folder_name.split(" / "))
