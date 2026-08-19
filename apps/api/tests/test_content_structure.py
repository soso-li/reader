from reader_api.digest import clean_preview, digest_score, first_markdown_image_url, split_digest_items, strip_html


def test_strip_html_preserves_paragraphs_and_images() -> None:
    html = """
    <h2>小标题</h2>
    <p>第一段</p>
    <p><img src="/cover.jpg" alt="封面"></p>
    <p>第二段</p>
    """

    text = strip_html(html, "https://example.com/post/1")

    assert "## 小标题\n" in text
    assert "第一段\n" in text
    assert "![封面](https://example.com/cover.jpg)" in text
    assert "\n第二段" in text


def test_strip_html_uses_lazy_image_sources() -> None:
    html = """
    <p><img src="data:image/gif;base64,AAAA" data-src="/real.jpg" alt="真实图"></p>
    <p><img data-original="https://cdn.example.com/original.jpg"></p>
    <p><img src="/t.png" data-original-src="/lazy-real.jpg" alt="懒加载图"></p>
    <p><img srcset="/small.jpg 1x, /large.jpg 2x" alt="srcset 图"></p>
    """

    text = strip_html(html, "https://example.com/post/1")

    assert "![真实图](https://example.com/real.jpg)" in text
    assert "![image](https://cdn.example.com/original.jpg)" in text
    assert "![懒加载图](https://example.com/lazy-real.jpg)" in text
    assert "![srcset 图](https://example.com/small.jpg)" in text
    assert "https://example.com/t.png" not in text
    assert "data:image" not in text


def test_strip_html_preserves_list_markers_for_reading_and_digest_score() -> None:
    html = """
    <ul>
      <li>OpenAI 发布新模型</li>
      <li>Nvidia 推出新芯片</li>
      <li>Anthropic 更新 Claude</li>
    </ul>
    """

    text = strip_html(html)

    assert text.splitlines() == ["- OpenAI 发布新模型", "- Nvidia 推出新芯片", "- Anthropic 更新 Claude"]
    assert digest_score("AI 周报", text) >= 0.55


def test_normal_content_item_keeps_paragraphs_and_images() -> None:
    body = "第一段\n![封面](https://example.com/cover.jpg)\n第二段"

    item = split_digest_items("普通文章", body, 0.0)[0]

    assert item["content_text"] == body
    assert item["summary"] == "第一段 第二段"


def test_clean_preview_removes_complete_and_truncated_images() -> None:
    text = "开头 ![封面](https://example.com/a.jpg) 中间 ![截断](https://example.com/"

    assert clean_preview(text) == "开头 中间"
    assert clean_preview("段落末尾被截断 !") == "段落末尾被截断"


def test_first_markdown_image_ignores_placeholder_and_alt_brackets() -> None:
    text = "![占位](https://example.com/t.png)\n![发现频道[2026] 41](https://example.com/real.jpg)"

    assert first_markdown_image_url(text) == "https://example.com/real.jpg"
