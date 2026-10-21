"""Compare every fixed-width field and reconstruct the source bytes."""

import argparse
import base64
import hashlib
import json
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent


def verify(source, output):
    source, output = Path(source), Path(output)
    manifest = json.loads((output / "summary.json").read_text())
    parts = sorted(output.glob("flights_part_*.parquet"))
    frame = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    positions = [
        (0, 60),
        (60, 72),
        (72, 84),
        (84, 96),
        (96, 108),
        (108, 120),
        (120, 130),
        (130, 132),
        (132, 138),
        (138, 144),
        (144, 150),
        (150, 168),
        (168, 180),
        (180, 186),
        (186, 192),
        (192, 198),
        (198, 204),
        (204, 216),
        (216, 240),
        (240, 276),
        (276, 336),
    ]
    names = list(frame.columns[:21])
    expected = pd.read_fwf(
        source,
        colspecs=positions,
        names=names,
        skiprows=1,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )
    pd.testing.assert_frame_equal(
        frame[names].astype("string"), expected.astype("string")
    )
    with source.open("rb") as handle:
        digest = hashlib.sha256(handle.readline())
        sha = hashlib.file_digest(source.open("rb"), "sha256").hexdigest()
    for row in frame.itertuples(index=False):
        data = base64.b64decode(row.raw_bytes_b64)
        digest.update(data)
        assert data.decode("utf8").rstrip("\r\n") == row.raw_line
        assert (
            row.record_id
            == hashlib.sha256(f"{sha}:{row.source_line}".encode()).hexdigest()
        )
        assert row.source_sha256 == sha
    assert digest.hexdigest() == sha == manifest["source_sha256"]
    assert frame.record_id.nunique() == len(frame) == manifest["rows_written"]
    assert frame.source_line.tolist() == list(range(2, len(frame) + 2))
    result = dict(
        all_checks_passed=True,
        rows=len(frame),
        fields_compared=21,
        source_bytes_reconstructed=True,
        independent_reader="pandas.read_fwf",
        source_sha256=sha,
    )
    (output / "verification.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=BASE_DIR.parents[1] / "raw_data/Sirena-export-fixed.tab",
    )
    p.add_argument("output", nargs="?", type=Path, default=BASE_DIR / "result")
    args = p.parse_args()
    print(json.dumps(verify(args.source, args.output), indent=2))
