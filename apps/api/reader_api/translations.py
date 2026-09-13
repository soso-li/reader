import json
import re

from sqlalchemy import select, text as sql_text
from sqlalchemy.orm import Session

from .digest import content_hash
from .llm import LLMProvider, merge_duplicate_json_keys
from .models import LLMTask, now_utc

TRANSLATION_PROMPT_VERSION = "translation-v3"
TRANSLATION_BLOCK_PROMPT_VERSION = "translation-blocks-v2"
TRANSLATION_TASK_TYPE = "translation:text"
TRANSLATION_CHUNK_CHAR_LIMIT = 3500
TRANSLATION_TARGET = "Simplified Chinese"
TRANSLATION_SEPARATOR = "\n\n%%\n\n"
SINGLE_TRANSLATION_ATTEMPTS = 3
TRANSLATION_MULTI_PROMPT = "Translate to {{to}}:\n\n{{text}}"
TRANSLATION_SINGLE_PROMPT = (
    "Translate to {{to}} (output translation only):\n\n{{text}}"
)
GARBLED_TRANSLATION_RE = re.compile(r"[\u3040-\u30ff\uac00-\ud7af]")
TRANSLATION_SYSTEM_PROMPT = """You are a professional {{to}} native translator who needs to fluently translate text into {{to}}.

## Translation Rules
1. Output only the translated content, without explanations or additional content (such as "Here's the translation:" or "Translation as follows:")
2. The returned translation must maintain exactly the same number of paragraphs and format as the original text
3. If the text contains HTML tags, consider where the tags should be placed in the translation while maintaining fluency
4. For content that should not be translated (such as proper nouns, code, etc.), keep the original text.
5. If input contains %%, use %% in your output, if input has no %%, don't use %% in your output{{title_prompt}}{{summary_prompt}}{{terms_prompt}}

## OUTPUT FORMAT:
- **Single paragraph input** → Output translation directly (no separators, no extra text)
- **Multi-paragraph input** → Use %% as paragraph separator between translations

## Examples
### Multi-paragraph Input:
Paragraph A

%%

Paragraph B

%%

Paragraph C

%%

Paragraph D

### Multi-paragraph Output:
Translation A

%%

Translation B

%%

Translation C

%%

Translation D

### Single paragraph Input:
Single paragraph content

### Single paragraph Output:
Direct translation without separators

{{imt_style_guide}}""".replace("{{to}}", TRANSLATION_TARGET)
TRANSLATION_URL_RE = re.compile(
    r"(?:https?://|www\.)\S+|"
    r"(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:[/:?#]\S*)?",
    re.IGNORECASE,
)


def ensure_translation(session: Session, provider: LLMProvider | None, model: str, text_value: str) -> str:
    text = text_value.strip()
    if not text or not needs_translation(text):
        return ""
    provider_name = translation_provider_name(provider)
    source_hash = content_hash(text)
    cached = cached_translation_text_for_hash(session, provider_name, model, source_hash)
    if cached:
        return cached
    if provider is None or not model:
        return ""
    acquire_translation_lock(session, content_hash(provider_name, model, source_hash))
    cached = cached_translation_text_for_hash(session, provider_name, model, source_hash)
    if cached:
        return cached
    translation = translate_text(provider, model, text)
    if not translation:
        return ""
    task = LLMTask(task_type=TRANSLATION_TASK_TYPE, provider=provider_name, object_type="text", object_id=translation_object_id(source_hash))
    task.status = "complete"
    task.prompt_version = TRANSLATION_PROMPT_VERSION
    task.model_version = model
    task.result_json = json.dumps({"source_hash": source_hash, "translation": translation}, ensure_ascii=False)
    task.updated_at = now_utc()
    session.add(task)
    session.flush()
    return translation


def translate_text(provider: LLMProvider, model: str, text: str) -> str:
    chunks = translation_chunks(text)
    translations: list[str] = []
    for chunk in chunks:
        paragraphs = [
            paragraph.strip()
            for paragraph in re.split(r"\n\s*\n", chunk)
            if paragraph.strip()
        ]
        translated = translate_segments(provider, model, paragraphs)
        if not translated:
            return ""
        translations.extend(translated)
    return "\n\n".join(translations).strip()


def ensure_block_translation(
    session: Session,
    provider: LLMProvider | None,
    model: str,
    blocks: list[dict[str, str]],
) -> LLMTask | None:
    clean = [
        {"id": block["id"], "text": block["text"].strip()}
        for block in blocks
        if block["text"].strip()
    ]
    if not clean:
        return None
    source_hash = block_translation_source_hash(clean)
    expected_ids = [block["id"] for block in clean]
    provider_name = translation_provider_name(provider)
    cached = latest_block_translation_task(
        session, provider_name, model, source_hash, expected_ids
    )
    if cached is not None:
        return cached
    if provider is None or not model:
        return None
    acquire_translation_lock(
        session, content_hash(provider_name, model, source_hash)
    )
    cached = latest_block_translation_task(
        session, provider_name, model, source_hash, expected_ids
    )
    if cached is not None:
        return cached
    translated, combined_translation = translate_blocks(provider, model, clean)
    if not translated:
        return None
    task = LLMTask(
        task_type=TRANSLATION_TASK_TYPE,
        provider=provider_name,
        object_type="text",
        object_id=translation_object_id(source_hash),
    )
    task.status = "complete"
    task.prompt_version = TRANSLATION_BLOCK_PROMPT_VERSION
    task.model_version = model
    task.result_json = json.dumps(
        {
            "source_hash": source_hash,
            "translation": combined_translation,
            "blocks": translated,
        },
        ensure_ascii=False,
    )
    task.updated_at = now_utc()
    session.add(task)
    session.flush()
    return task


def translate_blocks(
    provider: LLMProvider,
    model: str,
    blocks: list[dict[str, str]],
) -> tuple[list[dict[str, str]], str]:
    translated: list[dict[str, str]] = []
    for batch in block_translation_chunks(blocks):
        if len(batch) == 1 and len(translation_input([batch[0]["text"]])) > TRANSLATION_CHUNK_CHAR_LIMIT:
            text = translate_text(provider, model, batch[0]["text"])
            if not text:
                return [], ""
            translated.append({"id": batch[0]["id"], "text": text})
            continue
        values = translate_segments(
            provider,
            model,
            [block["text"] for block in batch],
        )
        if not values:
            return [], ""
        translated.extend(
            {"id": block["id"], "text": value}
            for block, value in zip(batch, values, strict=True)
        )
    return translated, "\n\n".join(block["text"] for block in translated)


def translate_segments(
    provider: LLMProvider,
    model: str,
    texts: list[str],
) -> list[str]:
    if not texts:
        return []
    if len(texts) == 1:
        translated = translate_single(provider, model, texts[0])
        return [translated] if translated else []
    if "%%" not in "".join(texts):
        try:
            result = provider.chat(
                model,
                TRANSLATION_SYSTEM_PROMPT,
                translation_input(texts),
            )
        except Exception:
            return []
        translated = parse_translation_output(result, texts)
        if translated:
            return translated
    repaired = [translate_single(provider, model, text) for text in texts]
    return repaired if all(repaired) else []


def translate_single(provider: LLMProvider, model: str, text: str) -> str:
    for _ in range(SINGLE_TRANSLATION_ATTEMPTS):
        try:
            result = provider.chat(
                model,
                TRANSLATION_SYSTEM_PROMPT,
                translation_input([text]),
            )
        except Exception:
            # Translation is a derived display cache; provider retries own transient failures.
            return ""
        translated = parse_translation_output(result, [text])
        if translated:
            return translated[0]
    return ""


def block_translation_chunks(
    blocks: list[dict[str, str]],
) -> list[list[dict[str, str]]]:
    chunks: list[list[dict[str, str]]] = []
    current: list[dict[str, str]] = []
    for block in blocks:
        candidate = [*current, block]
        if current and len(
            translation_input([candidate_block["text"] for candidate_block in candidate])
        ) > TRANSLATION_CHUNK_CHAR_LIMIT:
            chunks.append(current)
            current = [block]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def translation_input(texts: list[str]) -> str:
    text = TRANSLATION_SEPARATOR.join(texts)
    template = TRANSLATION_SINGLE_PROMPT if len(texts) == 1 else TRANSLATION_MULTI_PROMPT
    return template.replace("{{to}}", TRANSLATION_TARGET).replace("{{text}}", text)


def parse_translation_output(
    result: dict[str, object],
    source_texts: list[str],
) -> list[str]:
    output = translation_text(result)
    values = (
        [output]
        if len(source_texts) == 1
        else [value.strip() for value in output.split("%%")]
    )
    if len(values) != len(source_texts):
        return []
    if any(not value or looks_garbled_translation(value) for value in values):
        return []
    if len(source_texts) == 1 and "%%" not in source_texts[0] and "%%" in output:
        return []
    return values


def strip_translation_urls(value: str) -> str:
    return re.sub(r"\s+", " ", TRANSLATION_URL_RE.sub(" ", value)).strip()


def block_translation_source_hash(blocks: list[dict[str, str]]) -> str:
    return content_hash(
        json.dumps({"blocks": blocks}, ensure_ascii=False, separators=(",", ":"))
    )


def find_pending_translation_task(
    session: Session,
    provider_name: str,
    model: str,
    source_hash: str,
    prompt_version: str,
) -> LLMTask | None:
    """同一翻译身份的未完成任务（#106 异步化：轮询去重，不重复入队）。"""
    return session.scalars(
        select(LLMTask)
        .where(
            LLMTask.task_type == TRANSLATION_TASK_TYPE,
            LLMTask.object_type == "text",
            LLMTask.object_id == translation_object_id(source_hash),
            LLMTask.status == "pending",
            LLMTask.provider == provider_name,
            LLMTask.model_version == model,
            LLMTask.prompt_version == prompt_version,
        )
        .order_by(LLMTask.id.desc())
    ).first()


def latest_error_translation_task(
    session: Session,
    provider_name: str,
    model: str,
    source_hash: str,
    prompt_version: str,
) -> LLMTask | None:
    return session.scalars(
        select(LLMTask)
        .where(
            LLMTask.task_type == TRANSLATION_TASK_TYPE,
            LLMTask.object_type == "text",
            LLMTask.object_id == translation_object_id(source_hash),
            LLMTask.status == "error",
            LLMTask.provider == provider_name,
            LLMTask.model_version == model,
            LLMTask.prompt_version == prompt_version,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    ).first()


def create_pending_translation_task(
    session: Session,
    provider_name: str,
    model: str,
    *,
    text: str | None = None,
    blocks: list[dict[str, str]] | None = None,
    source_id: int | None = None,
) -> LLMTask:
    """建 pending 翻译任务；输入（含来源隐私推导所需 source_id）暂存于
    result_json，由 worker 作业消费。"""
    if blocks is not None:
        source_hash = block_translation_source_hash(blocks)
        prompt_version = TRANSLATION_BLOCK_PROMPT_VERSION
        payload: dict[str, object] = {"source_hash": source_hash, "input_blocks": blocks}
    else:
        assert text is not None
        source_hash = content_hash(text)
        prompt_version = TRANSLATION_PROMPT_VERSION
        payload = {"source_hash": source_hash, "input_text": text}
    if source_id is not None:
        payload["source_id"] = source_id
    task = LLMTask(
        task_type=TRANSLATION_TASK_TYPE,
        provider=provider_name,
        object_type="text",
        object_id=translation_object_id(source_hash),
    )
    task.status = "pending"
    task.prompt_version = prompt_version
    task.model_version = model
    task.result_json = json.dumps(payload, ensure_ascii=False)
    task.updated_at = now_utc()
    session.add(task)
    session.flush()
    return task


def run_pending_translation_task(
    session: Session, task: LLMTask, provider: LLMProvider | None
) -> None:
    """在 worker 内执行 pending 翻译任务并落定 complete/error 终态。

    LLM 调用只发生在这里；advisory 锁与二次缓存检查从旧的请求内路径
    原样迁移，用于对相同身份的并发作业去重。"""
    try:
        data = json.loads(task.result_json or "{}")
    except json.JSONDecodeError:
        data = {}
    source_hash = str(data.get("source_hash") or "")
    if not source_hash or provider is None or not task.model_version:
        _finalize_translation_error(task, source_hash, "翻译设置不可用或任务输入损坏")
        return
    acquire_translation_lock(
        session, content_hash(task.provider, task.model_version, source_hash)
    )
    if task.prompt_version == TRANSLATION_BLOCK_PROMPT_VERSION:
        blocks = data.get("input_blocks")
        if not isinstance(blocks, list) or not blocks:
            _finalize_translation_error(task, source_hash, "翻译任务缺少块输入")
            return
        expected_ids = [str(block.get("id")) for block in blocks]
        cached = latest_block_translation_task(
            session, task.provider, task.model_version, source_hash, expected_ids
        )
        if cached is not None:
            task.result_json = cached.result_json
            task.status = "complete"
            task.updated_at = now_utc()
            return
        try:
            translated, combined_translation = translate_blocks(
                provider, task.model_version, blocks
            )
        except Exception:
            translated, combined_translation = [], ""
        if not translated:
            _finalize_translation_error(task, source_hash, "翻译模型未返回可用译文")
            return
        task.result_json = json.dumps(
            {
                "source_hash": source_hash,
                "translation": combined_translation,
                "blocks": translated,
            },
            ensure_ascii=False,
        )
    else:
        text = str(data.get("input_text") or "")
        if not text.strip():
            _finalize_translation_error(task, source_hash, "翻译任务缺少文本输入")
            return
        cached = latest_translation_task(
            session, task.provider, task.model_version, source_hash
        )
        if cached is not None:
            task.result_json = cached.result_json
            task.status = "complete"
            task.updated_at = now_utc()
            return
        try:
            translation = translate_text(provider, task.model_version, text)
        except Exception:
            translation = ""
        if not translation:
            _finalize_translation_error(task, source_hash, "翻译模型未返回可用译文")
            return
        task.result_json = json.dumps(
            {"source_hash": source_hash, "translation": translation},
            ensure_ascii=False,
        )
    task.status = "complete"
    task.updated_at = now_utc()


def _finalize_translation_error(task: LLMTask, source_hash: str, message: str) -> None:
    task.status = "error"
    task.result_json = json.dumps(
        {"source_hash": source_hash, "error": message}, ensure_ascii=False
    )
    task.updated_at = now_utc()


def latest_block_translation_task(
    session: Session,
    provider_name: str,
    model: str,
    source_hash: str,
    expected_ids: list[str],
) -> LLMTask | None:
    provider_names = (
        ("local", "local-chat") if provider_name == "local" else (provider_name,)
    )
    rows = session.scalars(
        select(LLMTask)
        .where(
            LLMTask.task_type == TRANSLATION_TASK_TYPE,
            LLMTask.object_type == "text",
            LLMTask.object_id == translation_object_id(source_hash),
            LLMTask.status == "complete",
            LLMTask.provider.in_(provider_names),
            LLMTask.model_version == model,
            LLMTask.prompt_version == TRANSLATION_BLOCK_PROMPT_VERSION,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    ).all()
    for task in rows:
        try:
            data = json.loads(task.result_json)
        except json.JSONDecodeError:
            continue
        if data.get("source_hash") == source_hash and block_translation_result(
            task, expected_ids
        ):
            return task
    return None


def block_translation_result(
    task: LLMTask,
    expected_ids: list[str],
) -> list[dict[str, str]]:
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        return []
    blocks = data.get("blocks")
    if not isinstance(blocks, list) or len(blocks) != len(expected_ids):
        return []
    result: list[dict[str, str]] = []
    for expected_id, block in zip(expected_ids, blocks, strict=True):
        if (
            not isinstance(block, dict)
            or block.get("id") != expected_id
            or not isinstance(block.get("text"), str)
            or not block["text"].strip()
        ):
            return []
        result.append({"id": expected_id, "text": block["text"].strip()})
    return result


def looks_garbled_translation(text: str) -> bool:
    compact = normalize_text(text)
    if not compact:
        return False
    matches = GARBLED_TRANSLATION_RE.findall(compact)
    return len(matches) > 6 and len(matches) / max(len(compact), 1) > 0.35


def translation_chunks(text: str, limit: int = TRANSLATION_CHUNK_CHAR_LIMIT) -> list[str]:
    clean = text.strip()
    if len(translation_input([clean])) <= limit:
        return [clean] if clean else []
    chunks: list[str] = []
    current: list[str] = []
    single_limit = max(1, limit - len(translation_input([""])))
    for block in re.split(r"\n\s*\n", clean):
        paragraph = block.strip()
        if not paragraph:
            continue
        for part in split_long_paragraph(paragraph, single_limit):
            candidate = [*current, part]
            if current and len(translation_input(candidate)) > limit:
                chunks.append("\n\n".join(current))
                current = []
            current.append(part)
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def split_long_paragraph(paragraph: str, limit: int) -> list[str]:
    if len(paragraph) <= limit:
        return [paragraph]
    return [paragraph[start : start + limit].strip() for start in range(0, len(paragraph), limit) if paragraph[start : start + limit].strip()]


def cached_translation_text(session: Session, model: str, text_value: str, provider_name: str = "local") -> str:
    text = text_value.strip()
    if not text:
        return ""
    return cached_translation_text_for_hash(session, provider_name, model, content_hash(text))


def cached_translation_texts(
    session: Session,
    model: str,
    text_values: list[str],
    provider_name: str = "local",
) -> dict[str, str]:
    hashes = {text: content_hash(text.strip()) for text in text_values if text.strip()}
    if not hashes:
        return {}
    wanted_hashes = set(hashes.values())
    provider_names = ("local", "local-chat") if provider_name == "local" else (provider_name,)
    rows = session.scalars(
        select(LLMTask)
        .where(
            LLMTask.task_type == TRANSLATION_TASK_TYPE,
            LLMTask.object_type == "text",
            LLMTask.object_id.in_({translation_object_id(source_hash) for source_hash in wanted_hashes}),
            LLMTask.status == "complete",
            LLMTask.provider.in_(provider_names),
            LLMTask.model_version == model,
            LLMTask.prompt_version == TRANSLATION_PROMPT_VERSION,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    ).all()
    translations: dict[str, str] = {}
    for task in rows:
        try:
            data = json.loads(task.result_json)
        except json.JSONDecodeError:
            continue
        source_hash = data.get("source_hash")
        translation = data.get("translation")
        if source_hash in wanted_hashes and source_hash not in translations and isinstance(translation, str):
            translations[source_hash] = translation.strip()
    return {text: translations.get(source_hash, "") for text, source_hash in hashes.items()}


def cached_translation_text_for_hash(session: Session, provider_name: str, model: str, source_hash: str) -> str:
    task = latest_translation_task(session, provider_name, model, source_hash)
    if task is None:
        return ""
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        return ""
    translation = data.get("translation")
    return translation.strip() if isinstance(translation, str) else ""


def acquire_translation_lock(session: Session, source_hash: str) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    session.execute(sql_text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": translation_lock_key(source_hash)})


def latest_translation_task(session: Session, provider_name: str, model: str, source_hash: str) -> LLMTask | None:
    cache_provider_names = ("local", "local-chat") if provider_name == "local" else (provider_name,)
    rows = session.scalars(
        select(LLMTask)
        .where(
            LLMTask.task_type == TRANSLATION_TASK_TYPE,
            LLMTask.object_type == "text",
            LLMTask.object_id == translation_object_id(source_hash),
            LLMTask.status == "complete",
            LLMTask.provider.in_(cache_provider_names),
            LLMTask.model_version == model,
            LLMTask.prompt_version == TRANSLATION_PROMPT_VERSION,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    ).all()
    for task in rows:
        try:
            data = json.loads(task.result_json)
        except json.JSONDecodeError:
            continue
        if data.get("source_hash") == source_hash and isinstance(data.get("translation"), str):
            return task
    return None


def translation_provider_name(provider: LLMProvider | None) -> str:
    value = getattr(provider, "provider_name", "local")
    return value if value in {"local", "openai_compatible"} else "local"


def needs_translation(text: str) -> bool:
    return language_bucket(text) != "zh" and bool(normalize_text(text))


def needs_reading_translation(text: str) -> bool:
    clean = re.sub(
        r"(?:https?://|www\.)\S+|(?<![\w@])(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:[/:?#]\S*)?",
        " ",
        normalize_text(text),
        flags=re.IGNORECASE,
    )
    han = len(re.findall(r"[\u3400-\u9fff]", clean))
    kana = len(re.findall(r"[\u3041-\u3096\u30a1-\u30fa]", clean))
    hangul = len(re.findall(r"[\uac00-\ud7a3]", clean))
    latin = len(re.findall(r"[A-Za-z]", clean))
    other_letters = sum(character.isalpha() for character in clean) - han - kana - hangul - latin
    return (kana >= 4 and kana >= han) or (hangul >= 4 and hangul >= han) or (
        latin >= 8 and latin > han * 5 and latin > kana and latin > hangul
    ) or (
        other_letters >= 8
        and other_letters > han * 5
        and other_letters > latin
        and other_letters > kana
        and other_letters > hangul
    )


def language_bucket(text: str) -> str:
    clean = normalize_text(text)
    if re.search(r"[\u3040-\u30ff]", clean):
        return "ja"
    if re.search(r"[\uac00-\ud7af]", clean):
        return "ko"
    latin = len(re.findall(r"[A-Za-z]", clean))
    cjk = len(re.findall(r"[\u3400-\u9fff]", clean))
    if cjk and cjk * 5 >= latin:
        return "zh"
    if latin >= 8:
        return "latin"
    return "other"


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"!\[[^\]]*]\([^)]*(?:\)|$)", " ", text or "")).strip()


def translation_object_id(source_hash: str) -> int:
    return int(source_hash[:12], 16) % 2_147_483_647


def translation_lock_key(source_hash: str) -> int:
    value = int(source_hash[:16], 16)
    if value >= 2**63:
        value -= 2**64
    return value


def translation_text(result: dict[str, object]) -> str:
    text_value = llm_text(result)
    try:
        parsed = json.loads(unfence_json(text_value), object_pairs_hook=merge_duplicate_json_keys)
    except json.JSONDecodeError:
        return text_value
    if isinstance(parsed, dict):
        for key in ("translation", "text", "content", "answer"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return text_value


def llm_text(result: dict[str, object]) -> str:
    for key in ("output", "text", "reply", "response", "content"):
        value = result.get(key)
        if isinstance(value, str):
            return value.strip()
        if key == "output" and isinstance(value, list):
            for item in reversed(value):
                if isinstance(item, dict) and item.get("type") == "message" and isinstance(item.get("content"), str):
                    return item["content"].strip()
    choices = result.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                return message["content"].strip()
            if isinstance(first.get("text"), str):
                return first["text"].strip()
    return ""


def unfence_json(value: str) -> str:
    text = value.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()
