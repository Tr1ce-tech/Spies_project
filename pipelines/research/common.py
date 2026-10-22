import hashlib
import json
import re
import unicodedata
from pathlib import Path

VERSION = "source-research.v1"
MARKERS = {"", "N/A", "NA", "NULL", "NONE", "NAN", "-"}


def clean(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFC", str(value)).strip()
    return None if value.upper() in MARKERS else value


def name(value):
    value = clean(value)
    return " ".join(value.upper().split()) if value else None


def document(value):
    value = clean(value)
    if value is None:
        return None
    if re.fullmatch(r"[0-9 ]+", value):
        return value.replace(" ", "")
    return value


def airport(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFC", str(value)).strip().upper()
    return value or None


def card(value):
    value = clean(value)
    match = re.fullmatch(r"(?:FF#)?([A-Z0-9]{2,8})\s+([0-9]+)", value or "")
    return match.groups() if match else (None, None)


def script(value):
    value = value or ""
    latin = bool(re.search(r"[A-Z]", value))
    cyrillic = bool(re.search(r"[А-ЯЁ]", value))
    return (
        "mixed"
        if latin and cyrillic
        else "latin"
        if latin
        else "cyrillic"
        if cyrillic
        else "other"
    )


def sha(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def install_functions(db):
    for label, function in [
        ("clean_text", clean),
        ("name_text", name),
        ("doc_key", document),
        ("name_script", script),
        ("airport_text", airport),
        ("card_program_key", lambda x: card(x)[0]),
        ("card_number_key", lambda x: card(x)[1]),
    ]:
        db.create_function(
            label, function, ["VARCHAR"], "VARCHAR", null_handling="special"
        )


def export(db, query, path):
    db.execute(f"COPY ({query}) TO {literal(path)} (FORMAT PARQUET, COMPRESSION ZSTD)")


def dump(path, obj):
    Path(path).write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, default=str) + "\n"
    )
