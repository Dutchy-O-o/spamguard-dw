"""
download_enron_spam.py
----------------------
Downloads the Metsis/Androutsopoulos/Paliouras 2006 "Enron-Spam" dataset.

Source: aueb.gr/users/ion/data/enron-spam/preprocessed/
Files : enron1.tar.gz .. enron6.tar.gz  (preprocessed / bodies-only variant)

Target directory:
    SpamData/enron-spam-original/enron1/spam/*.txt
                                        /ham/*.txt
                                enron2/...

- Skips each tar.gz if it has already been downloaded and extracted.
- Uses urllib.request with the default SSL context.
"""
from __future__ import annotations

import ssl
import sys
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "SpamData" / "enron-spam-original"
TARGET.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www2.aueb.gr/users/ion/data/enron-spam/preprocessed/"
DATASETS = [f"enron{i}.tar.gz" for i in range(1, 7)]


def _do_download(url: str, dst: Path, ctx) -> None:
    with urllib.request.urlopen(url, context=ctx, timeout=120) as r, dst.open("wb") as f:
        total = int(r.headers.get("Content-Length", 0))
        read = 0
        while True:
            chunk = r.read(65536)
            if not chunk:
                break
            f.write(chunk)
            read += len(chunk)
            if total:
                pct = 100 * read / total
                print(f"    {dst.name}: {read/1e6:6.1f} / {total/1e6:6.1f} MB ({pct:5.1f}%)",
                      end="\r")
        print(" " * 70, end="\r")


def download(url: str, dst: Path) -> None:
    # On Windows the system certificate store is often empty — fallback required.
    try:
        _do_download(url, dst, ssl.create_default_context())
    except Exception as e:
        print(f"    [ssl fallback] {type(e).__name__}: {e}")
        _do_download(url, dst, ssl._create_unverified_context())


def main() -> None:
    archives_dir = TARGET / "_archives"
    archives_dir.mkdir(exist_ok=True)

    for name in DATASETS:
        url      = BASE_URL + name
        tar_path = archives_dir / name
        out_dir  = TARGET / name.replace(".tar.gz", "")

        if out_dir.exists() and any(out_dir.rglob("*.txt")):
            print(f"[skip] {name} already extracted -> {out_dir}")
            continue

        if not tar_path.exists():
            print(f"[dl]   {url}")
            try:
                download(url, tar_path)
            except Exception as e:
                print(f"[ERROR] download: {e}")
                sys.exit(1)
        else:
            print(f"[cache] {tar_path}")

        print(f"[unpack] {tar_path.name} -> {out_dir.relative_to(ROOT)}")
        with tarfile.open(tar_path, "r:gz") as tf:
            tf.extractall(TARGET)

    # summary
    total_spam = len(list(TARGET.rglob("spam/*.txt")))
    total_ham  = len(list(TARGET.rglob("ham/*.txt")))
    print(f"\n[ok] spam files : {total_spam:,}")
    print(f"[ok] ham  files : {total_ham:,}")
    print(f"[ok] target dir : {TARGET}")


if __name__ == "__main__":
    main()
