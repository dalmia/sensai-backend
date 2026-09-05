"""Convert cached Notion blocks into native SensAI (BlockNote) blocks.

One-off migration for the Notion integration removal (2026-09-05). Committed
here rather than left in an ops folder because sensai-frontend#54 is only safe
if this has run: it removes the `notion` render path, so any surviving `notion`
block would otherwise reach an editor whose schema has no spec for it.

Applied to production 2026-09-05: 462 tasks + 360 questions, 0 leftover notion
blocks. Backup taken first with VACUUM INTO ->
  /appdata_prod/db.backup-pre-notion-convert.sqlite
Rollback: stop the containers, restore that file over /appdata_prod/db.sqlite.

Run without --apply first; it prints exactly what it would change.

Reads the `notion` wrapper blocks stored in tasks.blocks / questions.blocks and
rewrites them as normal editor blocks, using the content already cached in the
database. No Notion API access and no OAuth token required.

Usage:
    python notion_to_blocks.py --db path/to/db.sqlite            # dry run
    python notion_to_blocks.py --db path/to/db.sqlite --apply    # write
"""

import argparse
import json
import os
import sqlite3
import urllib.request
import uuid
from collections import Counter

# block_id -> SensAI media URL, populated from the rescue manifest (see
# notion_media_rescue.py / notion_media_upload.py). Notion's own media URLs
# expire within the hour, so every cached one is dead; these point at our S3.
MEDIA_URLS = {}


def load_media_urls(manifest_path, api_base):
    """Resolve a SensAI URL for each rescued+uploaded file, keyed by notion block id."""
    if not manifest_path:
        return 0
    manifest = json.load(open(os.path.expanduser(manifest_path)))
    resolved = 0
    for m in manifest:
        if not m.get("s3_uuid"):
            continue
        url = (
            f"{api_base}/file/presigned-url/get"
            f"?uuid={m['s3_uuid']}&file_extension={m['s3_ext']}"
        )
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                MEDIA_URLS[m["block_id"]] = json.loads(r.read())["url"]
                resolved += 1
        except Exception as e:
            print(f"  could not resolve url for block {m['block_id']}: {e}")
    return resolved

TEXT_PROPS = {
    "textColor": "default",
    "backgroundColor": "default",
    "textAlignment": "left",
}

# Exact prop sets used by the existing BlockNote content, sampled from tasks that
# were never linked to Notion. Converted blocks must match these or the editor
# will not load them for editing.
PROP_TEMPLATES = {
    "paragraph": dict(TEXT_PROPS),
    "heading": {**TEXT_PROPS, "level": 1},
    "bulletListItem": dict(TEXT_PROPS),
    "numberedListItem": dict(TEXT_PROPS),
    "checkListItem": {**TEXT_PROPS, "checked": False},
    "quote": {"backgroundColor": "default", "textColor": "default"},
    "toggleListItem": {"textColor": "default", "backgroundColor": "default"},
    "codeBlock": {"language": "text"},
    "image": {
        "backgroundColor": "default",
        "textAlignment": "left",
        "name": "",
        "url": "",
        "caption": "",
        "showPreview": True,
        "previewWidth": 512,
    },
    "video": {
        "backgroundColor": "default",
        "textAlignment": "left",
        "name": "",
        "url": "",
        "caption": "",
        "showPreview": True,
        "previewWidth": 512,
    },
    "audio": {
        "backgroundColor": "default",
        "name": "",
        "url": "",
        "caption": "",
        "showPreview": True,
    },
}

HEADING_LEVEL = {"heading_1": 1, "heading_2": 2, "heading_3": 3}

SIMPLE_MAP = {
    "paragraph": "paragraph",
    "bulleted_list_item": "bulletListItem",
    "numbered_list_item": "numberedListItem",
    "quote": "quote",
    "toggle": "toggleListItem",
    "callout": "paragraph",
    "template": "paragraph",
}

MEDIA_MAP = {"image": "image", "video": "video", "file": "video", "pdf": "video"}


def new_id():
    return str(uuid.uuid4())


def styles_from(annotations):
    if not annotations:
        return {}
    s = {}
    for key in ("bold", "italic", "underline", "code"):
        if annotations.get(key):
            s[key] = True
    if annotations.get("strikethrough"):
        s["strike"] = True
    colour = annotations.get("color")
    if colour and colour != "default":
        s["textColor"] = colour
    return s


def rich_text_to_content(rich_text):
    """Notion rich_text -> BlockNote inline content, preserving styles and links."""
    out = []
    for rt in rich_text or []:
        text = rt.get("plain_text")
        if text is None:
            text = (rt.get("text") or {}).get("content", "")
        if not text:
            continue
        node = {"type": "text", "text": text, "styles": styles_from(rt.get("annotations"))}
        href = rt.get("href") or ((rt.get("text") or {}).get("link") or {}).get("url")
        if href:
            out.append({"type": "link", "href": href, "content": [node]})
        else:
            out.append(node)
    return out


def media_url(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("file", "external"):
        v = payload.get(key)
        if isinstance(v, dict) and v.get("url"):
            return v["url"]
    return payload.get("url")


def block(btype, content=None, props=None, children=None):
    template = PROP_TEMPLATES.get(btype, dict(TEXT_PROPS))
    return {
        "id": new_id(),
        "type": btype,
        "props": {**template, **(props or {})},
        "content": content if content is not None else [],
        "children": children or [],
    }


CELL_PROPS = {
    "backgroundColor": "default",
    "textColor": "default",
    "textAlignment": "left",
}


def table_to_blocks(payload, stats):
    """Notion table -> ' | ' separated paragraphs.

    SensAI removes `table` from the BlockNote schema (BlockNoteEditor.tsx:185,189),
    so a real table block crashes the editor. Rows are flattened instead: the
    content survives and stays editable.
    """
    out = []
    for row in payload.get("table_rows") or []:
        cells = (row.get("table_row") or {}).get("cells") or row.get("cells") or []
        parts = []
        for cell in cells:
            text = "".join(
                rt.get("plain_text") or (rt.get("text") or {}).get("content", "")
                for rt in (cell or [])
            )
            parts.append(text.strip())
        line = " | ".join(parts).strip(" |").strip()
        if line:
            out.append(
                block("paragraph", [{"type": "text", "text": line, "styles": {}}])
            )
    stats["table_rows_flattened"] += len(out)
    return out


def link_paragraph(url, label=None):
    return block(
        "paragraph",
        [
            {
                "type": "link",
                "href": url,
                "content": [{"type": "text", "text": label or url, "styles": {}}],
            }
        ],
    )


def convert_block(nb, stats, allow_media=True):
    """One Notion block -> list of BlockNote blocks.

    allow_media=False is used for questions: QuizEditor renders them with
    allowMedia={false}, so image/video/audio are not in that editor's schema and
    would crash it. Those become links instead.
    """
    ntype = nb.get("type")
    payload = nb.get(ntype) if isinstance(nb.get(ntype), dict) else {}
    # `items` covers the bulleted_list / numbered_list grouping wrappers emitted
    # by the renderer, which hold the real *_list_item blocks.
    kids = (
        (nb.get("children") or [])
        + (payload.get("children") or [])
        + (payload.get("items") or [])
    )

    def children_blocks():
        out = []
        for k in kids:
            out.extend(convert_block(k, stats, allow_media))
        return out

    if ntype in ("bulleted_list", "numbered_list"):
        stats[ntype] += 1
        return children_blocks()

    if ntype in HEADING_LEVEL:
        stats[ntype] += 1
        return [
            block(
                "heading",
                rich_text_to_content(payload.get("rich_text")),
                {"level": HEADING_LEVEL[ntype]},
                children_blocks(),
            )
        ]

    if ntype == "code":
        stats["code"] += 1
        lang = payload.get("language") or "text"
        return [
            block(
                "codeBlock",
                rich_text_to_content(payload.get("rich_text")),
                {"language": lang},
            )
        ]

    if ntype == "to_do":
        stats["to_do"] += 1
        return [
            block(
                "checkListItem",
                rich_text_to_content(payload.get("rich_text")),
                {"checked": bool(payload.get("checked"))},
                children_blocks(),
            )
        ]

    if ntype == "divider":
        # Purely visual and BlockNote has no divider block - drop it rather than
        # leaving thousands of empty paragraphs for admins to clean up.
        stats["divider_dropped"] += 1
        return []

    if ntype == "table":
        stats["table"] += 1
        return table_to_blocks(payload, stats)

    if ntype in MEDIA_MAP:
        url = MEDIA_URLS.get(nb.get("id")) or media_url(payload)
        if MEDIA_URLS.get(nb.get("id")):
            stats[f"{ntype}_rehosted"] += 1
        stats[ntype] += 1
        if not url:
            return []
        caption = rich_text_to_content(payload.get("caption"))
        name = "".join(n.get("text", "") for n in caption if isinstance(n, dict))
        if not allow_media:
            stats[f"{ntype}_as_link"] += 1
            return [link_paragraph(url, name or None)]
        return [block(MEDIA_MAP[ntype], [], {"url": url, "caption": name, "name": name})]

    if ntype == "audio":
        url = MEDIA_URLS.get(nb.get("id")) or media_url(payload)
        if MEDIA_URLS.get(nb.get("id")):
            stats["audio_rehosted"] += 1
        stats["audio"] += 1
        if not url:
            return []
        if not allow_media:
            stats["audio_as_link"] += 1
            return [link_paragraph(url)]
        return [block("audio", [], {"url": url, "name": url.rsplit("/", 1)[-1]})]

    if ntype in ("embed", "bookmark", "link_preview"):
        url = payload.get("url") or media_url(payload)
        stats[ntype] += 1
        if not url:
            return []
        return [
            block(
                "paragraph",
                [{"type": "link", "href": url, "content": [{"type": "text", "text": url, "styles": {}}]}],
            )
        ]

    if ntype in ("column_list", "column", "synced_block", "table_of_contents"):
        # Structural wrappers - flatten their children.
        stats[ntype] += 1
        return children_blocks()

    if ntype in SIMPLE_MAP:
        stats[ntype] += 1
        return [
            block(
                SIMPLE_MAP[ntype],
                rich_text_to_content(payload.get("rich_text")),
                None,
                children_blocks(),
            )
        ]

    # Unknown type: keep any text we can find rather than dropping content.
    stats[f"UNMAPPED:{ntype}"] += 1
    content = rich_text_to_content(payload.get("rich_text")) if payload else []
    return [block("paragraph", content)] if content else []


def convert_document(blocks, stats, allow_media=True):
    """Replace every `notion` wrapper in a document with converted blocks."""
    out = []
    changed = False
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "notion":
            changed = True
            cached = b.get("content") or []
            if not cached:
                stats["empty_cache_blanked"] += 1
                out.append(block("paragraph"))
                continue
            for nb in cached:
                out.extend(convert_block(nb, stats, allow_media))
        else:
            out.append(b)
    for i, b in enumerate(out):
        if isinstance(b, dict):
            b["position"] = i
    return out, changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--media-manifest", help="manifest.json from the media rescue")
    ap.add_argument("--api", default="http://localhost:8010", help="backend base url")
    args = ap.parse_args()

    if args.media_manifest:
        n = load_media_urls(args.media_manifest, args.api)
        print(f"resolved {n} rehosted media url(s)\n")

    conn = sqlite3.connect(args.db)
    stats = Counter()
    touched = Counter()
    samples = []

    for table in ("tasks", "questions"):
        rows = list(conn.execute(f"SELECT id, blocks FROM {table} WHERE blocks IS NOT NULL"))
        for rid, raw in rows:
            try:
                blocks = json.loads(raw)
            except Exception:
                continue
            if not isinstance(blocks, list):
                continue
            if not any(isinstance(b, dict) and b.get("type") == "notion" for b in blocks):
                continue

            converted, changed = convert_document(
                blocks, stats, allow_media=(table == "tasks")
            )
            if not changed:
                continue
            touched[table] += 1
            if len(samples) < 3:
                samples.append((table, rid, blocks, converted))
            if args.apply:
                conn.execute(
                    f"UPDATE {table} SET blocks = ? WHERE id = ?",
                    (json.dumps(converted), rid),
                )

    if args.apply:
        conn.commit()

    print("=== ROWS CONVERTED ===")
    for t, n in touched.items():
        print(f"  {t:<12} {n}")

    print()
    print("=== BLOCKS PRODUCED BY SOURCE TYPE ===")
    for k, v in sorted(stats.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<28} {v}")

    unmapped = [k for k in stats if k.startswith("UNMAPPED:")]
    print()
    print("UNMAPPED TYPES:", unmapped or "none")

    print()
    print("=== SAMPLE (first converted doc) ===")
    if samples:
        table, rid, before, after = samples[0]
        print(f"{table} id={rid}: {len(before)} block(s) -> {len(after)} block(s)")
        for b in after[:4]:
            text = "".join(
                c.get("text", "") for c in (b.get("content") or []) if isinstance(c, dict)
            )
            print(f"  {b['type']:<18} {text[:70]!r}")

    print()
    print("APPLIED" if args.apply else "DRY RUN - nothing written")


if __name__ == "__main__":
    main()
