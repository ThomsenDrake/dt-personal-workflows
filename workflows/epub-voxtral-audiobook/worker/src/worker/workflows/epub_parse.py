"""Pure-stdlib .epub parsing: zip -> OPF -> spine -> chapter texts.

No third-party imports so the module is unit-testable without the SDK.
"""
from __future__ import annotations

import io
import posixpath
import re
import zipfile
from html.parser import HTMLParser
from xml.etree import ElementTree as ET

XHTML_MEDIA_TYPES = {"application/xhtml+xml", "text/html"}
_BLOCK_TAGS = {
    "p", "div", "section", "article", "blockquote", "li", "tr", "table",
    "h1", "h2", "h3", "h4", "h5", "h6", "title", "header", "footer", "main", "aside",
}
_SKIP_TAGS = {"script", "style", "head"}
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_NL_RE = re.compile(r"\n{3,}")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._skip_depth = 0
        self._heading: list[str] = []
        self._in_heading = False

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag in _BLOCK_TAGS:
            self._chunks.append("\n")
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._in_heading = True
            self._heading = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag in _BLOCK_TAGS:
            self._chunks.append("\n")
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6") and self._in_heading:
            self._in_heading = False
            self._heading.append("")

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not data.strip():
            return
        if self._in_heading:
            self._heading.append(data)
        self._chunks.append(data)

    def text(self) -> str:
        raw = "".join(self._chunks)
        raw = _WS_RE.sub(" ", raw)
        lines = [line.strip() for line in raw.split("\n")]
        out = _NL_RE.sub("\n\n", "\n".join(line for line in lines if line))
        return out.strip()

    def first_heading(self) -> str:
        return _WS_RE.sub(" ", "".join(self._heading)).strip()


def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _find_text(elem, name: str) -> str:
    for child in elem.iter():
        if _localname(child.tag) == name and child.text:
            return child.text.strip()
    return ""


def _container_rootfile(zf: zipfile.ZipFile) -> str:
    try:
        root = ET.fromstring(zf.read("META-INF/container.xml"))
    except KeyError:
        raise ValueError("not an epub: META-INF/container.xml is missing")
    except ET.ParseError as exc:
        raise ValueError(f"not an epub: container.xml does not parse: {exc}")
    for elem in root.iter():
        if _localname(elem.tag) == "rootfile":
            path = elem.get("full-path")
            if path:
                return path
    raise ValueError("not an epub: no rootfile in container.xml")


def _parse_opf(zf: zipfile.ZipFile, opf_path: str) -> dict:
    try:
        root = ET.fromstring(zf.read(opf_path))
    except KeyError:
        raise ValueError(f"not an epub: rootfile {opf_path!r} missing from zip")
    except ET.ParseError as exc:
        raise ValueError(f"not an epub: {opf_path!r} does not parse: {exc}")

    base = posixpath.dirname(opf_path)
    metadata = next((e for e in root.iter() if _localname(e.tag) == "metadata"), None)
    title = _find_text(metadata, "title") if metadata is not None else ""
    author = _find_text(metadata, "creator") if metadata is not None else ""

    manifest: dict[str, tuple[str, str]] = {}
    manifest_elem = next((e for e in root.iter() if _localname(e.tag) == "manifest"), None)
    if manifest_elem is not None:
        for item in manifest_elem:
            if _localname(item.tag) != "item":
                continue
            item_id = item.get("id") or ""
            href = item.get("href") or ""
            media = (item.get("media-type") or "").lower()
            if item_id and href:
                manifest[item_id] = (posixpath.normpath(posixpath.join(base, href)), media)

    spine: list[str] = []
    spine_elem = next((e for e in root.iter() if _localname(e.tag) == "spine"), None)
    if spine_elem is not None:
        for itemref in spine_elem:
            if _localname(itemref.tag) != "itemref":
                continue
            ref = itemref.get("idref") or ""
            if ref in manifest:
                spine.append(ref)
    return {"title": title, "author": author, "manifest": manifest, "spine": spine}


def _chapter_text(zf: zipfile.ZipFile, path: str) -> tuple[str, str]:
    raw = zf.read(path)
    extractor = _TextExtractor()
    try:
        extractor.feed(raw.decode("utf-8", errors="replace"))
        extractor.close()
    except Exception:
        text = _WS_RE.sub(" ", raw.decode("utf-8", errors="replace"))
        return text.strip(), ""
    return extractor.text(), extractor.first_heading()


def parse_epub(data: bytes, max_book_chars: int = 1_500_000) -> dict:
    """Parse epub bytes into {title, author, chapters: [{title, text}], total_chars}."""
    if not data:
        raise ValueError("empty epub: no bytes")
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not an epub: zip does not open: {exc}")
    with zf:
        opf = _parse_opf(zf, _container_rootfile(zf))
        chapters: list[dict] = []
        total = 0
        for i, ref in enumerate(opf["spine"]):
            path, media = opf["manifest"][ref]
            if media and media not in XHTML_MEDIA_TYPES:
                continue
            try:
                text, heading = _chapter_text(zf, path)
            except KeyError:
                continue
            if not text:
                continue
            title = heading or posixpath.basename(path)
            if total + len(text) > max_book_chars:
                remaining = max_book_chars - total
                if remaining <= 0:
                    chapters.append({
                        "title": title,
                        "text": "",
                        "truncated": True,
                    })
                    break
                text = text[:remaining]
                chapters.append({"title": title, "text": text, "truncated": True})
                total += len(text)
                break
            chapters.append({"title": title, "text": text, "truncated": False})
            total += len(text)
        if not chapters:
            raise ValueError("epub has no readable text chapters (spine empty or non-XHTML)")
        return {
            "title": opf["title"] or "untitled",
            "author": opf["author"],
            "chapters": chapters,
            "total_chars": total,
        }
