import xml.etree.ElementTree as ET

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from reader_api.db import Base
from reader_api.digest import content_hash, normalize_title
from reader_api.models import ClusterItem, ContentItem, Document, Folder, RawEntry, Source
from reader_api.opml import export_opml, import_opml
from tests.factories import make_raw_entry


def test_import_export_opml_with_folder() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Tech">
        <outline type="rss" text="Example" xmlUrl="https://example.com/rss.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 1
        assert session.scalar(select(Folder).where(Folder.name == "Tech")) is not None
        assert session.scalar(select(Source).where(Source.url == "https://example.com/rss.xml")) is not None
        exported = export_opml(session)

    assert "https://example.com/rss.xml" in exported
    assert "Tech" in exported
    assert "Reader 订阅源" in exported


def test_import_opml_infers_media_type_from_folo_folders() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Pictures">
        <outline type="rss" text="Image Feed" xmlUrl="https://example.com/image.xml" />
      </outline>
      <outline text="Videos">
        <outline type="rss" text="Video Feed" xmlUrl="https://example.com/video.xml" />
      </outline>
      <outline text="Audios">
        <outline type="rss" text="Audio Feed" xmlUrl="https://example.com/audio.xml" />
      </outline>
      <outline text="Notifications">
        <outline type="rss" text="Notification Feed" xmlUrl="https://example.com/notification.xml" />
      </outline>
      <outline text="SocialMedia">
        <outline text="Twitter">
          <outline type="rss" text="Social Feed" xmlUrl="https://example.com/social.xml" />
        </outline>
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 5
        media_types = {source.name: source.media_type for source in session.scalars(select(Source)).all()}

    assert media_types == {
        "Audio Feed": "podcast",
        "Image Feed": "image",
        "Notification Feed": "notification",
        "Social Feed": "social",
        "Video Feed": "video",
    }


def test_opml_reader_media_type_has_priority_and_round_trips_same_named_folders() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Shared" readerMediaType="social">
        <outline type="rss" text="Uses Folder Type" xmlUrl="https://example.com/social.xml" />
        <outline type="rss" text="Uses Source Type" xmlUrl="https://example.com/video.xml" readerMediaType="video" />
      </outline>
      <outline text="Shared" readerMediaType="video">
        <outline type="rss" text="Video Folder" xmlUrl="https://example.com/video-folder.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 3
        folders = session.scalars(
            select(Folder).where(Folder.name == "Shared").order_by(Folder.media_type)
        ).all()
        sources = {
            source.name: source
            for source in session.scalars(select(Source)).all()
        }
        exported = export_opml(session)

    assert [(folder.name, folder.media_type) for folder in folders] == [
        ("Shared", "social"),
        ("Shared", "video"),
    ]
    assert sources["Uses Folder Type"].media_type == "social"
    assert sources["Uses Folder Type"].folder_id == folders[0].id
    assert sources["Uses Source Type"].media_type == "video"
    assert sources["Uses Source Type"].folder_id is None
    root = ET.fromstring(exported)
    exported_folders = root.findall("./body/outline[@text='Shared']")
    assert {node.attrib["readerMediaType"] for node in exported_folders} == {"social", "video"}
    exported_sources = root.findall(".//outline[@xmlUrl]")
    assert {node.attrib["readerMediaType"] for node in exported_sources} == {
        "social",
        "video",
    }


def test_import_opml_keeps_empty_folders_and_moves_duplicates() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Empty" />
      <outline text="Moved">
        <outline type="rss" text="Example" xmlUrl="https://example.com/rss.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        session.add(Source(name="Example", url="https://example.com/rss.xml"))
        session.commit()

        assert import_opml(session, xml) == 0
        empty = session.scalar(select(Folder).where(Folder.name == "Empty"))
        moved = session.scalar(select(Folder).where(Folder.name == "Moved"))
        source = session.scalar(select(Source).where(Source.url == "https://example.com/rss.xml"))
        exported = export_opml(session)

    assert empty is not None
    assert moved is not None
    assert source is not None
    assert source.folder_id == moved.id
    assert "Empty" in exported
    assert exported.count("https://example.com/rss.xml") == 1


def test_import_export_opml_preserves_nested_folder_paths() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Parent">
        <outline text="Child">
          <outline type="rss" text="Nested" xmlUrl="https://example.com/nested.xml" />
        </outline>
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 1
        folders = session.scalars(select(Folder.name).order_by(Folder.name)).all()
        source = session.scalar(select(Source).where(Source.url == "https://example.com/nested.xml"))
        source_folder_name = session.get(Folder, source.folder_id).name if source else ""
        exported = export_opml(session)

    assert folders == ["Parent", "Parent / Child"]
    assert source is not None
    assert source_folder_name == "Parent / Child"
    body = ET.fromstring(exported).find("body")
    assert body is not None
    parent = body.find("./outline[@text='Parent']")
    assert parent is not None
    child = parent.find("./outline[@text='Child']")
    assert child is not None
    assert child.find("./outline[@xmlUrl='https://example.com/nested.xml']") is not None


def test_import_opml_bounds_names_to_postgres_columns() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    outer_name = "外" * 140
    inner_name = "内" * 140
    source_name = "源" * 321
    xml = f"""<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="{outer_name}">
        <outline text="{inner_name}">
          <outline type="rss" text="{source_name}" xmlUrl="https://example.com/long.xml" />
        </outline>
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 1
        source = session.scalar(select(Source))
        folder_names = session.scalars(select(Folder.name)).all()

    assert source is not None
    assert len(source.name) == 320
    assert max(map(len, folder_names)) == 240


def test_import_opml_strips_redundant_articles_root_from_real_folder_name() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Articles">
        <outline text="Science">
          <outline type="rss" text="Science Feed" xmlUrl="https://example.com/science.xml" />
        </outline>
      </outline>
    </body></opml>
    """

    with Session() as session:
        assert import_opml(session, xml) == 1
        source = session.scalar(select(Source).where(Source.url == "https://example.com/science.xml"))
        assert source is not None
        assert session.get(Folder, source.folder_id).name == "Science"


def test_import_opml_trims_urls_and_ignores_blank_feeds() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Tech">
        <outline type="rss" text="Example" xmlUrl=" https://example.com/rss.xml " />
        <outline type="rss" text="Blank" xmlUrl="   " />
        <outline type="rss" text="Local" xmlUrl="file:///tmp/feed.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        session.add(Source(name="Existing", url="https://example.com/rss.xml"))
        session.commit()

        assert import_opml(session, xml) == 0
        folder = session.scalar(select(Folder).where(Folder.name == "Tech"))
        sources = session.scalars(select(Source)).all()
        exported = export_opml(session)

    assert folder is not None
    assert len(sources) == 1
    assert sources[0].folder_id == folder.id
    assert "Blank" not in exported
    assert "Local" not in exported


def test_import_opml_matches_existing_url_after_normalization() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Tech">
        <outline type="rss" text="Example" xmlUrl="https://example.com/rss.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        session.add(Source(name="Existing", url="HTTPS://EXAMPLE.COM/rss.xml#old"))
        session.commit()

        assert import_opml(session, xml) == 0
        sources = session.scalars(select(Source)).all()
        folder = session.scalar(select(Folder).where(Folder.name == "Tech"))

    assert len(sources) == 1
    assert sources[0].url == "https://example.com/rss.xml"
    assert folder is not None
    assert sources[0].folder_id == folder.id


def test_import_opml_reactivates_archived_duplicate() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    xml = """<?xml version="1.0"?>
    <opml version="2.0"><body>
      <outline text="Tech">
        <outline type="rss" text="Archived" xmlUrl="https://example.com/rss.xml" />
      </outline>
    </body></opml>
    """

    with Session() as session:
        source = Source(name="Old archived name", url="https://example.com/rss.xml", status="archived", enabled=False)
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="archived-1", title="Archived story", content_hash=content_hash("Archived story"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="Archived story", content_text="Archived body")
        session.add(document)
        session.flush()
        session.add(
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title="Archived story",
                summary="Archived body",
                content_text="Archived body",
                content_hash=content_hash("Archived body"),
                normalized_title=normalize_title("Archived story"),
            )
        )
        session.commit()

        assert import_opml(session, xml) == 0
        source = session.scalar(select(Source).where(Source.url == "https://example.com/rss.xml"))
        folder = session.scalar(select(Folder).where(Folder.name == "Tech"))
        cluster_links = session.scalars(select(ClusterItem)).all()

    assert source is not None
    assert source.name == "Archived"
    assert source.status == "active"
    assert source.enabled is True
    assert folder is not None
    assert source.folder_id == folder.id
    assert cluster_links == []


def test_export_opml_skips_archived_and_deleted_sources() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        folder = Folder(name="Tech")
        session.add(folder)
        session.flush()
        session.add_all(
            [
                Source(name="Active", url="https://example.com/active.xml", folder_id=folder.id),
                Source(name="Archived", url="https://example.com/archived.xml", folder_id=folder.id, status="archived", enabled=False),
                Source(name="Deleted", url="https://example.com/deleted.xml", folder_id=folder.id, status="deleted", enabled=False),
                Source(name="Loose Archived", url="https://example.com/loose-archived.xml", status="archived", enabled=False),
                Source(name="Loose Deleted", url="https://example.com/loose-deleted.xml", status="deleted", enabled=False),
            ]
        )
        session.commit()
        exported = export_opml(session)

    assert "https://example.com/active.xml" in exported
    assert "https://example.com/archived.xml" not in exported
    assert "https://example.com/deleted.xml" not in exported
    assert "https://example.com/loose-archived.xml" not in exported
    assert "https://example.com/loose-deleted.xml" not in exported
    assert "Tech" in exported
