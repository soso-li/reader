from __future__ import annotations

import xml.etree.ElementTree as ET
from io import StringIO

from sqlalchemy import select
from sqlalchemy.orm import Session

from .cluster import cluster_source_items, decluster_source_items
from .clustering_run import (
    clustering_run_execution_lock,
    defer_clustering_run_lock_until_transaction_end,
)
from .models import DELETED_SOURCE_STATUS, Folder, Source
from .media_types import (
    SOURCE_MEDIA_TYPES,
    media_type_from_legacy_folder_path,
    normalize_folder_name,
)
from .source_url import clean_source_url, source_by_url


def import_opml(session: Session, xml_text: str) -> int:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError("OPML 格式无效") from exc
    body = root.find("body")
    if body is None:
        return 0
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        count = 0
        for outline in body:
            count += import_outline(session, outline, None, [], None)
        session.commit()
        return count


def import_outline(
    session: Session,
    node: ET.Element,
    folder: Folder | None,
    folder_path: list[str],
    inherited_folder_media_type: str | None,
) -> int:
    raw_xml_url = node.attrib.get("xmlUrl") or node.attrib.get("xmlurl")
    try:
        xml_url = clean_source_url(raw_xml_url) if raw_xml_url is not None else ""
    except ValueError:
        return 0
    title = clean_label(
        node.attrib.get("title") or node.attrib.get("text") or xml_url or "未命名"
    )[:320]
    if xml_url:
        media_type = (
            reader_media_type(node)
            or (folder.media_type if folder is not None else None)
            or media_type_from_legacy_folder_path(folder_path)
        )
        folder_id = folder.id if folder is not None and folder.media_type == media_type else None
        exists = source_by_url(session, xml_url)
        if exists is None:
            session.add(
                Source(
                    name=title,
                    url=xml_url,
                    folder_id=folder_id,
                    media_type=media_type,
                )
            )
            return 1
        session.refresh(exists, with_for_update=True)
        exists.url = xml_url
        old_media_type = exists.media_type
        exists.media_type = media_type
        exists.folder_id = folder_id
        if old_media_type == "article" and media_type != "article":
            decluster_source_items(session, exists.id)
        elif old_media_type != "article" and media_type == "article" and exists.status == "active" and exists.enabled:
            cluster_source_items(session, exists.id)
        if exists.status == "archived":
            exists.name = title
            exists.status = "active"
            exists.enabled = True
            if media_type == "article":
                cluster_source_items(session, exists.id)
        return 0

    next_path = [*folder_path, title]
    declared_media_type = reader_media_type(node)
    folder_media_type = (
        declared_media_type
        or inherited_folder_media_type
        or media_type_from_legacy_folder_path(next_path)
    )
    folder_name = normalize_folder_name(
        folder_name_for_path(next_path),
        folder_media_type,
    )[:240]
    next_folder = session.scalar(
        select(Folder).where(
            Folder.name == folder_name,
            Folder.media_type == folder_media_type,
        )
    )
    if next_folder is None:
        next_folder = Folder(name=folder_name, media_type=folder_media_type)
        session.add(next_folder)
        session.flush()
    children = list(node)
    inherited_media_type = declared_media_type or inherited_folder_media_type
    return sum(
        import_outline(
            session,
            child,
            next_folder,
            next_path,
            inherited_media_type,
        )
        for child in children
    )


def clean_label(value: str) -> str:
    return value.strip() or "未命名"


def reader_media_type(node: ET.Element) -> str | None:
    value = node.attrib.get("readerMediaType", "").strip()
    return value if value in SOURCE_MEDIA_TYPES else None


def export_opml(session: Session) -> str:
    opml = ET.Element("opml", {"version": "2.0"})
    head = ET.SubElement(opml, "head")
    ET.SubElement(head, "title").text = "Reader 订阅源"
    body = ET.SubElement(opml, "body")

    folders = session.scalars(
        select(Folder).order_by(Folder.media_type, Folder.name)
    ).all()
    used_source_ids: set[int] = set()
    folder_nodes: dict[tuple[str, ...], ET.Element] = {}
    for folder in folders:
        folder_node = ensure_folder_node(
            body,
            folder_nodes,
            folder.media_type,
            folder_path_parts(folder.name),
        )
        for source in sorted(folder.sources, key=lambda item: item.name):
            if not export_source(source):
                continue
            used_source_ids.add(source.id)
            ET.SubElement(
                folder_node,
                "outline",
                source_outline_attributes(source),
            )

    loose_sources = session.scalars(select(Source).where(Source.folder_id.is_(None)).order_by(Source.name)).all()
    for source in loose_sources:
        if source.id in used_source_ids or not export_source(source):
            continue
        ET.SubElement(body, "outline", source_outline_attributes(source))

    out = StringIO()
    ET.ElementTree(opml).write(out, encoding="unicode", xml_declaration=True)
    return out.getvalue()


def folder_name_for_path(path: list[str]) -> str:
    return " / ".join(path)


def export_source(source: Source) -> bool:
    return source.status not in {"archived", DELETED_SOURCE_STATUS}


def source_outline_attributes(source: Source) -> dict[str, str]:
    return {
        "type": "rss",
        "text": source.name,
        "title": source.name,
        "xmlUrl": source.url,
        "htmlUrl": source.site_url,
        "readerMediaType": source.media_type,
    }


def folder_path_parts(name: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in name.split(" / ") if part.strip()) or (name,)


def ensure_folder_node(
    body: ET.Element,
    nodes: dict[tuple[str, ...], ET.Element],
    media_type: str,
    path: tuple[str, ...],
) -> ET.Element:
    parent = body
    for index, part in enumerate(path, 1):
        key = (media_type, *path[:index])
        node = nodes.get(key)
        if node is None:
            node = ET.SubElement(
                parent,
                "outline",
                {
                    "text": part,
                    "title": part,
                    "readerMediaType": media_type,
                },
            )
            nodes[key] = node
        parent = node
    return parent
