from reader_api.cluster import cluster_key_for
from reader_api.digest import content_hash, normalize_title
from reader_api.models import ContentItem


def test_cluster_key_uses_exact_identity_not_title() -> None:
    first = ContentItem(
        document_id=1,
        source_id=1,
        title="Nvidia announces new AI chip",
        content_hash=content_hash("a"),
        canonical_url="https://example.com/a",
        normalized_title=normalize_title("Nvidia announces new AI chip"),
    )
    second = ContentItem(
        document_id=2,
        source_id=2,
        title="Nvidia announces new AI chip",
        content_hash=content_hash("b"),
        canonical_url="https://another.example/b",
        normalized_title=normalize_title("Nvidia announces new AI chip"),
    )

    assert first.normalized_title == second.normalized_title
    assert cluster_key_for(first) != cluster_key_for(second)
