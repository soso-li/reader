from lxml import etree
from lxml.html import fromstring
import pytest

from reader_api.article_selectors import select_article_elements


@pytest.mark.parametrize("selector", ["article.content", "css:article.content"])
def test_article_selector_supports_legacy_and_explicit_css(selector: str) -> None:
    document = fromstring(
        '<main><article id="first" class="content">正文</article></main>'
    )

    assert [
        element.get("id")
        for element in select_article_elements(document, selector)
    ] == ["first"]


def test_article_selector_supports_xpath_1_0() -> None:
    document = fromstring(
        '<main><article id="first"/><article id="second"/></main>'
    )

    assert [
        element.get("id")
        for element in select_article_elements(
            document, 'xpath://article[@id="second"]'
        )
    ] == ["second"]


@pytest.mark.parametrize(
    "selector",
    [
        "xpath:string(//article)",
        "xpath://article/text()",
        "xpath://article/@href",
        "xpath://article/@href/.",
        "xpath://comment()",
        "xpath://processing-instruction()",
    ],
)
def test_article_selector_rejects_xpath_non_element_results(
    selector: str,
) -> None:
    document = etree.fromstring(
        "<main><!--comment--><?reader test?><article>正文</article></main>"
    )

    with pytest.raises(ValueError, match="元素节点"):
        select_article_elements(document, selector)


def test_article_selector_disables_xpath_extensions() -> None:
    document = fromstring("<main><article>正文</article></main>")

    with pytest.raises(ValueError, match="XPath"):
        select_article_elements(
            document,
            'xpath://article[re:test(string(.), "正文")]',
        )
