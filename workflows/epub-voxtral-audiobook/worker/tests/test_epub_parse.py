import io
import zipfile

from worker.workflows import epub_parse


def _make_epub(chapters: list[tuple[str, str]], title: str = "Test Book", author: str = "A. Author") -> bytes:
    """chapters: [(filename, xhtml body)]"""
    buf = io.BytesIO()
    items = "".join(
        f'<item id="c{i}" href="{name}" media-type="application/xhtml+xml"/>'
        for i, (name, _) in enumerate(chapters)
    )
    spine = "".join(f'<itemref idref="c{i}"/>' for i in range(len(chapters)))
    opf = f"""<?xml version="1.0"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title><dc:creator>{author}</dc:creator><dc:identifier id="id">x</dc:identifier>
  </metadata>
  <manifest>{items}</manifest>
  <spine>{spine}</spine>
</package>"""
    container = """<?xml version="1.0"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
  <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>
</container>"""
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("OEBPS/content.opf", opf)
        for name, body in chapters:
            zf.writestr(f"OEBPS/{name}", f'<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml"><body>{body}</body></html>')
    return buf.getvalue()


def test_parse_basic_epub():
    data = _make_epub([
        ("ch1.xhtml", "<h1>One</h1><p>Hello world.</p><p>Second paragraph.</p>"),
        ("ch2.xhtml", "<h2>Two</h2><p>Goodbye <b>world</b>.</p>"),
    ])
    book = epub_parse.parse_epub(data)
    assert book["title"] == "Test Book"
    assert book["author"] == "A. Author"
    assert len(book["chapters"]) == 2
    assert book["chapters"][0]["title"] == "One"
    assert "Hello world." in book["chapters"][0]["text"]
    assert "Second paragraph." in book["chapters"][0]["text"]
    assert "Goodbye world." in book["chapters"][1]["text"]
    assert book["total_chars"] == sum(len(c["text"]) for c in book["chapters"])


def test_parse_rejects_non_zip():
    try:
        epub_parse.parse_epub(b"this is not a zip")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_parse_rejects_zip_without_container():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("random.txt", "hi")
    try:
        epub_parse.parse_epub(buf.getvalue())
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "container.xml" in str(exc)


def test_parse_skips_non_xhtml_spine_items():
    buf = io.BytesIO()
    opf = """<?xml version="1.0"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>T</dc:title><dc:identifier id="id">x</dc:identifier></metadata>
  <manifest>
    <item id="img" href="img.png" media-type="image/png"/>
    <item id="c1" href="c1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine><itemref idref="img"/><itemref idref="c1"/></spine>
</package>"""
    container = """<?xml version="1.0"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
  <rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles>
</container>"""
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("META-INF/container.xml", container)
        zf.writestr("content.opf", opf)
        zf.writestr("img.png", b"\x89PNG")
        zf.writestr("c1.xhtml", '<html xmlns="http://www.w3.org/1999/xhtml"><body><p>Only text.</p></body></html>')
    book = epub_parse.parse_epub(buf.getvalue())
    assert len(book["chapters"]) == 1
    assert "Only text." in book["chapters"][0]["text"]


def test_max_book_chars_truncates():
    data = _make_epub([("ch1.xhtml", f"<p>{'x' * 5000}</p>")])
    book = epub_parse.parse_epub(data, max_book_chars=1000)
    assert book["chapters"][0]["truncated"] is True
    assert len(book["chapters"][0]["text"]) == 1000
    assert book["total_chars"] == 1000
