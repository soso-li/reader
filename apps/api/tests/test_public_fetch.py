from reader_api.public_fetch import fetch_public_bytes


def test_fetch_public_bytes_revalidates_redirect_target_before_request() -> None:
    class Redirect:
        status = 302
        headers = {"Location": "http://private.example/feed.xml"}

        def close(self) -> None:
            return None

    requested_addresses: list[str] = []

    def resolver(host: str, _port: int, _timeout: float) -> tuple[str, ...]:
        return (
            ("93.184.216.34",)
            if host == "example.com"
            else ("192.168.1.20",)
        )

    def request(**kwargs: object) -> Redirect:
        requested_addresses.append(str(kwargs["address"]))
        return Redirect()

    result = fetch_public_bytes(
        "https://example.com/feed.xml",
        max_bytes=1024,
        resolver=resolver,
        request=request,
        clock=lambda: 0.0,
    )

    assert result.succeeded is False
    assert result.diagnostic == "ssrf_blocked"
    assert requested_addresses == ["93.184.216.34"]


def test_fetch_public_bytes_trusts_only_the_configured_private_origin() -> None:
    class Feed:
        status = 200
        headers = {"Content-Type": "application/xml"}

        def read(self, _size: int) -> bytes:
            return b""

        def close(self) -> None:
            return None

    class Redirect(Feed):
        status = 302
        headers = {"Location": "http://192.0.2.7/feed.xml"}

    requested: list[tuple[str, str]] = []

    def request(**kwargs: object) -> Feed:
        requested.append((str(kwargs["url"]), str(kwargs["address"])))
        if str(kwargs["url"]).endswith("/redirect"):
            return Redirect()
        return Feed()

    allowed = fetch_public_bytes(
        "http://192.0.2.6:1200/claude/blog",
        max_bytes=1024,
        trusted_origin="http://192.0.2.6:1200",
        request=request,
    )
    blocked = fetch_public_bytes(
        "http://192.0.2.6:1201/claude/blog",
        max_bytes=1024,
        trusted_origin="http://192.0.2.6:1200",
        request=request,
    )
    redirected = fetch_public_bytes(
        "http://192.0.2.6:1200/redirect",
        max_bytes=1024,
        trusted_origin="http://192.0.2.6:1200",
        request=request,
    )

    assert allowed.succeeded is True
    assert blocked.diagnostic == "ssrf_blocked"
    assert redirected.diagnostic == "ssrf_blocked"
    assert requested == [
        ("http://192.0.2.6:1200/claude/blog", "192.0.2.6"),
        ("http://192.0.2.6:1200/redirect", "192.0.2.6"),
    ]
