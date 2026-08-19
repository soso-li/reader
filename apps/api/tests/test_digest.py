from reader_api.digest import DIGEST_SPLIT_THRESHOLD, clean_title, digest_score, document_type, lsh_signature, split_digest_items


def test_digest_score_classifies_morning_brief() -> None:
    body = """
    1. OpenAI 发布新模型 https://example.com/a
    2. 英伟达推出新芯片 https://example.com/b
    3. 某公司完成融资 https://example.com/c
    4. Microsoft 建设 AI 数据中心 https://example.com/d
    5. Google 发布开发者平台 https://example.com/e
    6. Apple 推出系统更新 https://example.com/f
    """
    score = digest_score("AI 早报：OpenAI、Nvidia、Anthropic、Microsoft、Google、Apple", body)

    assert score >= DIGEST_SPLIT_THRESHOLD
    assert document_type(score, "AI 早报：OpenAI、Nvidia、Anthropic、Microsoft、Google、Apple") == "digest"
    assert len(split_digest_items("AI 早报", body, score)) == 6


def test_digest_without_numbered_markers_stays_single_when_strict() -> None:
    body = """
    OpenAI 发布新模型，面向开发者开放新的推理能力 https://example.com/a

    Nvidia 推出新芯片，主打数据中心训练和推理场景 https://example.com/b

    Anthropic 更新 Claude，新增企业团队管理能力 https://example.com/c

    Microsoft 建设数据中心，扩大云端 AI 算力供给

    Google 发布平台能力，整合搜索、广告和开发者工具

    Apple 发布系统更新，强化端侧模型和隐私保护
    """
    title = "AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    score = digest_score(title, body)

    assert document_type(score, title) != "digest"
    assert len(split_digest_items(title, body, score)) == 1


def test_normal_article_stays_single_item() -> None:
    body = "这是一篇围绕同一个事件展开的普通文章，只有连续正文，没有多个编号条目。"
    score = digest_score("OpenAI releases a model", body)

    assert score < 0.40
    assert document_type(score, "OpenAI releases a model") == "normal_article"
    assert len(split_digest_items("OpenAI releases a model", body, score)) == 1


def test_mixed_score_keeps_original_article_unsplit() -> None:
    body = """
    育碧旗下开放世界合作射击游戏《全境封锁2》目前在 Steam 平台已不再锁国区，可正常购买。
    目前《全境封锁2》在 Steam 上还有促销活动，标准版价格 32 元。
    在《全境封锁2》里，玩家将驰骋于致命疫情后的华盛顿特区，保卫并重建城市：
    战术战斗 - 在紧张刺激的掩体枪战中迎战敌人。
    打造终极特工 - 搜刮并打造顶尖装备，学习强力技能。
    团队协作方能挽救生命 - 与最多三位其他特工组队执行战术合作任务。
    极致终局体验 - 达到最高等级仅是征程的开始。
    """

    items = split_digest_items("《全境封锁2》Steam 不再锁国区", body, 0.40)

    assert len(items) == 1
    assert "战术战斗" in items[0]["content_text"]
    assert "极致终局体验" in items[0]["content_text"]


def test_high_score_without_digest_title_signal_stays_unsplit() -> None:
    body = """
    1. 战术战斗 - 在紧张刺激的掩体枪战中迎战敌人。
    2. 打造终极特工 - 搜刮并打造顶尖装备，学习强力技能。
    3. 团队协作方能挽救生命 - 与最多三位其他特工组队执行任务。
    4. 极致终局体验 - 达到最高等级仅是征程的开始。
    https://example.com/a https://example.com/b https://example.com/c
    """
    title = "《全境封锁2》Steam 不再锁国区"
    score = DIGEST_SPLIT_THRESHOLD

    assert document_type(score, title) == "mixed"
    assert len(split_digest_items(title, body, score)) == 1


def test_related_links_do_not_turn_article_into_digest() -> None:
    body = """
    Samsung Galaxy Fold 新机曝光，正文围绕同一条产品爆料展开。
    该机重量、屏幕和电池规格来自同一个消息源。
    相关阅读：
    - 《上一条折叠屏爆料》
    - 《数据库现身记录》
    - 《电池和快充消息》
    """
    html = """
    <p>Samsung Galaxy Fold 新机曝光，正文围绕同一条产品爆料展开。</p>
    <p>相关阅读：</p>
    <p><a href="https://example.com/a">上一条折叠屏爆料</a></p>
    <p><a href="https://example.com/b">数据库现身记录</a></p>
    <p><a href="https://example.com/c">电池和快充消息</a></p>
    """
    score = digest_score("Samsung Galaxy Fold specs leaked", body, html)

    assert score < 0.40
    assert document_type(score, "Samsung Galaxy Fold specs leaked") == "normal_article"
    assert len(split_digest_items("Samsung Galaxy Fold specs leaked", body, score)) == 1


def test_related_html_links_do_not_push_borderline_article_to_mixed() -> None:
    body = """
    Samsung Galaxy Fold Android Snapdragon Qualcomm Ahmed Qwaider 爆料集中在同一款折叠屏手机。

    尺寸信息围绕同一条产品爆料展开，屏幕、电池和重量都来自相同消息源。

    配置信息继续描述该产品的芯片、内存、影像和发布时间，没有切换到其他事件。

    发布时间仍然指向同一场 Samsung Unpacked 活动。

    相关阅读：
    - 《上一条折叠屏爆料》
    - 《数据库现身记录》
    - 《电池和快充消息》
    """
    html = """
    <p>Samsung Galaxy Fold Android Snapdragon Qualcomm Ahmed Qwaider 爆料集中在同一款折叠屏手机。</p>
    <p>相关阅读：</p>
    <p><a href="https://example.com/a">上一条折叠屏爆料</a></p>
    <p><a href="https://example.com/b">数据库现身记录</a></p>
    <p><a href="https://example.com/c">电池和快充消息</a></p>
    """

    score = digest_score("Samsung / Galaxy / Fold specs", body, html)

    assert score < 0.40
    assert document_type(score, "Samsung / Galaxy / Fold specs") == "normal_article"


def test_inline_related_heading_does_not_split_related_items() -> None:
    body = """
    Samsung Galaxy Fold Android Snapdragon Qualcomm Ahmed Qwaider 爆料集中在同一款折叠屏手机。

    尺寸信息围绕同一条产品爆料展开，屏幕、电池和重量都来自相同消息源。

    发布时间仍然指向同一场 Samsung Unpacked 活动。

    相关阅读：上一条折叠屏爆料
    - 数据库现身记录提供更早备案信息和型号线索。
    - 电池和快充消息补充同一款产品的供应链传闻。
    - 屏幕折痕消息回顾此前围绕这款手机的爆料。
    """
    html = """
    <p>Samsung Galaxy Fold Android Snapdragon Qualcomm Ahmed Qwaider 爆料集中在同一款折叠屏手机。</p>
    <p>相关阅读：<a href="https://example.com/a">上一条折叠屏爆料</a></p>
    <p><a href="https://example.com/b">数据库现身记录</a></p>
    <p><a href="https://example.com/c">电池和快充消息</a></p>
    """

    score = digest_score("Samsung / Galaxy / Fold specs", body, html)

    assert score < 0.40
    assert document_type(score, "Samsung / Galaxy / Fold specs") == "normal_article"
    assert len(split_digest_items("Samsung / Galaxy / Fold specs", body, score)) == 1


def test_digest_score_counts_html_links() -> None:
    html = """
    <p><a href="https://example.com/a">one</a></p>
    <p><a href="https://example.com/b">two</a></p>
    <p><a href="https://example.com/c">three</a></p>
    """

    assert digest_score("Plain update", "single topic paragraph", html) == 0.10


def test_lsh_signature_is_stable_and_bounded() -> None:
    signature = lsh_signature("Nvidia announces chip", "Nvidia announces a new AI chip for servers.")

    assert signature == lsh_signature("Nvidia announces chip", "Nvidia announces a new AI chip for servers.")
    assert signature.startswith("b:")
    assert len(signature) == 258
    int(signature[2:], 16)


def test_digest_item_titles_do_not_use_images() -> None:
    body = """
    - ![cover](https://example.com/cover.jpg)
      Nvidia 推出新芯片，面向数据中心训练和推理。
    - OpenAI 发布新模型，面向开发者开放测试。
    """

    items = split_digest_items("<img src='x.jpg'> AI 早报", body, DIGEST_SPLIT_THRESHOLD)

    assert clean_title("<img src='x.jpg'> Actual title") == "Actual title"
    assert clean_title("![cover](https://example.com/cover.", "Fallback") == "Fallback"
    assert items[0]["title"] == "Nvidia 推出新芯片，面向数据中心训练和推理。"
    assert not items[0]["title"].startswith("![")
