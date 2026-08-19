from lxml.html import fragment_fromstring

from reader_api.reading_content import normalize_reading_content


def test_normalize_reading_content_preserves_structure_in_both_outputs() -> None:
    result = normalize_reading_content(
        """
        <article>
          <h2>章节标题</h2>
          <p>　　正文有 <strong>重点</strong>、<em>语气</em>和
            <a href="../source">来源链接</a>。</p>
          <blockquote><p>引用内容</p></blockquote>
          <ol><li>第一项</li><li>第二项</li></ol>
          <figure>
            <img src="/images/hero.jpg" alt="封面">
            <figcaption>图片说明</figcaption>
          </figure>
          <table><tr><th>项目</th><th>值</th></tr><tr><td>A</td><td>1</td></tr></table>
          <p>行内 <code>x = 1</code></p>
          <pre><code>print("ok")</code></pre>
        </article>
        """,
        "https://example.com/news/story",
    )

    document = fragment_fromstring(result.reading_html, create_parent="div")
    paragraph = document.xpath(".//p[contains(@class, 'reader-first-line-indent')]")
    image = document.xpath(".//img")[0]
    link = document.xpath(".//a")[0]

    assert len(paragraph) == 1
    assert link.get("href") == "https://example.com/source"
    assert image.get("src").startswith("/images/rss?")
    assert image.get("data-reader-original-src") == (
        "https://example.com/images/hero.jpg"
    )
    assert result.content_text == (
        "## 章节标题\n\n"
        "正文有 **重点**、*语气*和 [来源链接](https://example.com/source)。\n\n"
        "> 引用内容\n\n"
        "1. 第一项\n"
        "2. 第二项\n\n"
        "![封面](https://example.com/images/hero.jpg)\n\n"
        "图片说明\n\n"
        "| 项目 | 值 |\n"
        "| A | 1 |\n\n"
        "行内 `x = 1`\n\n"
        '```\nprint("ok")\n```'
    )


def test_normalize_reading_content_enforces_html_and_css_allowlists() -> None:
    result = normalize_reading_content(
        """
        <main onclick="steal()">
          <script>alert("script")</script>
          <style>body { display: none }</style>
          <form><input value="secret"><button>提交秘密</button></form>
          <iframe src="https://third.example/embed"></iframe>
          <p style="font-family: Arial, sans-serif; color: #123456;
                    background-color: rgb(1, 2, 3); font-size: 1.25em;
                    text-align: center; margin-left: 2em;
                    background-image: url(javascript:steal());
                    position: fixed"
             data-track="user">安全正文</p>
          <a href="javascript:alert(1)" onmouseover="steal()">危险链接文字</a>
          <img src="data:image/svg+xml,boom" onerror="steal()" alt="危险图片">
          <object data="https://third.example/object">对象内容</object>
          <font size="1">安全小字</font>
        </main>
        """,
        "https://example.com/story",
    )

    lowered = result.reading_html.lower()
    assert "script" not in lowered
    assert "onclick" not in lowered
    assert "onmouseover" not in lowered
    assert "onerror" not in lowered
    assert "<form" not in lowered
    assert "<iframe" not in lowered
    assert "<object" not in lowered
    assert "<input" not in lowered
    assert "javascript:" not in lowered
    assert "data:image" not in lowered
    assert "background-image" not in lowered
    assert "position:" not in lowered
    assert "data-track" not in lowered
    assert (
        'style="font-family:Arial, sans-serif;color:#123456;'
        "background-color:rgb(1, 2, 3);font-size:1.25em;"
        'text-align:center;margin-left:2em"'
    ) in result.reading_html
    assert "安全正文" in result.content_text
    assert "危险链接文字" in result.content_text
    assert "危险图片" in result.content_text
    assert 'style="font-size:.75em"' in result.reading_html
    assert "提交秘密" not in result.content_text
    assert "对象内容" not in result.content_text


def test_normalize_reading_content_assigns_deterministic_unique_block_ids() -> None:
    html = (
        "<p>重复段落</p><p><span style='color:red'>重复段落</span></p>"
        "<blockquote>直接引用</blockquote>"
    )

    first = normalize_reading_content(html, "https://example.com/a")
    second = normalize_reading_content(html, "https://example.com/a")
    document = fragment_fromstring(first.reading_html, create_parent="div")
    block_ids = document.xpath(
        ".//p/@data-reader-block-id | .//blockquote/*/@data-reader-block-id"
    )

    assert first == second
    assert len(block_ids) == 3
    assert len(set(block_ids)) == 3
    assert all(block_id.startswith("block-") for block_id in block_ids)


def test_normalize_reading_content_does_not_invent_chinese_indentation() -> None:
    result = normalize_reading_content(
        "<p>没有明确缩进证据的中文段落。</p>"
        "<p>&emsp;&emsp;明确缩进。</p>"
        "<p><span></span>&emsp;尾部明确缩进。</p>",
        "https://example.com/story",
    )
    document = fragment_fromstring(result.reading_html, create_parent="div")

    assert document.xpath(".//p[1]/@class") == []
    assert document.xpath(".//p[2]/@class") == ["reader-first-line-indent"]
    assert document.xpath(".//p[3]/@class") == ["reader-first-line-indent"]
    assert result.content_text == (
        "没有明确缩进证据的中文段落。\n\n明确缩进。\n\n尾部明确缩进。"
    )


def test_normalize_reading_content_filters_placeholder_images() -> None:
    result = normalize_reading_content(
        """
        <p>正文</p>
        <img src="/spacer.gif" alt="占位图">
        <img src="/tracking.png" width="1" height="1" alt="追踪像素">
        """,
        "https://example.com/story",
    )

    assert "<img" not in result.reading_html
    assert result.content_text == "正文"
