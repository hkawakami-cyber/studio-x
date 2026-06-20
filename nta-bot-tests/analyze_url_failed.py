#!/usr/bin/env python3
"""
url_failed ドメイン分析スクリプト
Mac 上の houjin.db で実行: python3 analyze_url_failed.py ~/Downloads/houjin.db

出力:
  - url_failed レコードのドメイン別集計（上位 N 件）
  - 既存 SKIP_DOMAINS に含まれていないドメインのみ
  - SKIP_DOMAINS に追加すべき候補を Python コードで出力
"""

import re
import sqlite3
import sys
from collections import Counter
from urllib.parse import urlparse

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "~/Downloads/houjin.db"
TOP_N    = int(sys.argv[2]) if len(sys.argv) > 2 else 50

SKIP_DOMAINS = frozenset([
    "google.", "bing.com", "yahoo.co.jp", "duckduckgo.com",
    "facebook.", "twitter.com", "x.com", "linkedin.",
    "freee.co.jp", "imijiten.net", "info.gbiz.go.jp", "cnavi.g-search.or.jp",
    "kotobank.jp", "baseconnect.in", "manareki.com", "data-link-plus.com",
    "kensetumap.com", "salesnow.jp", "g-search.or.jp", "houjin.com",
    "kigyolog.com", "uijin.com", "biz-maps.com", "navi-i.jp",
    "yayoi-kk.co.jp", "nabutan.com", "companydata.tsujigawa.com",
    "kaisharesearch.com", "houjin.info", "houjin.jp", "houjin.goo.to",
    "companyinformation.jp", "compalyze.co.jp", "korps.jp",
    "tsukulink.net", "kawasaki-connect.jp", "alarmbox.jp",
    "web.suke-dachi.jp", "toukibo.ai-con.lawyer",
    "weblio.jp", "ejje.weblio.jp", "kanji.jitenon.jp", "myoji-yurai.net",
    "navitime.co.jp", "mhlw.go.jp",
    "city.minato.tokyo.jp", "city.yokohama.lg.jp", "city.sapporo.jp",
    "reddit.com", "zhihu.com", "office.com", "athome.co.jp", "mens-aso.co.jp",
])

# 公的機関・汎用サービスのパターン（候補選定の参考）
PATTERNS_LIKELY_BAD = [
    r"houjin", r"kaisha", r"hojin", r"corp", r"company", r"企業",
    r"city\.", r"\.lg\.jp$", r"\.go\.jp$",
    r"wikipedia", r"amazon", r"rakuten", r"yahoo", r"google", r"bing",
    r"indeed", r"doda", r"rikunabi", r"mynavi",
]
PATTERN_RE = re.compile("|".join(PATTERNS_LIKELY_BAD), re.IGNORECASE)


def extract_domain(url: str) -> str | None:
    try:
        netloc = urlparse(url).netloc
        return netloc.removeprefix("www.") or None
    except Exception:
        return None


def is_already_skipped(domain: str) -> bool:
    return any(
        domain == d or domain.endswith("." + d) or d.endswith("." + domain)
        for d in SKIP_DOMAINS
    )


def main():
    import os
    path = os.path.expanduser(DB_PATH)
    print(f"\n接続: {path}")
    conn = sqlite3.connect(path, timeout=30)

    # ── 全体件数 ──────────────────────────────────────────────
    total_failed = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='url_failed'"
    ).fetchone()[0]
    total_error = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='error'"
    ).fetchone()[0]
    total_skip = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='skip'"
    ).fetchone()[0]
    print(f"url_failed: {total_failed:,}件 / error: {total_error:,}件 / skip: {total_skip:,}件\n")

    # ── url_failed の hp_url を収集 ───────────────────────────
    rows = conn.execute(
        "SELECT hp_url FROM crawl_queue WHERE status='url_failed' AND hp_url IS NOT NULL"
    ).fetchall()
    conn.close()

    domains = [extract_domain(r[0]) for r in rows if r[0]]
    domains = [d for d in domains if d]
    counter = Counter(domains)

    print(f"{'rank':>4}  {'件数':>8}  {'%':>5}  {'ドメイン':<45}  {'備考'}")
    print("-" * 90)

    new_candidates = []
    rank = 0
    shown = 0

    for domain, count in counter.most_common():
        if shown >= TOP_N:
            break
        already = is_already_skipped(domain)
        likely_bad = bool(PATTERN_RE.search(domain))
        note = ""
        if already:
            note = "★ 既にSKIP"
        elif likely_bad:
            note = "→ 要確認（企業情報DB系の可能性）"

        rank += 1
        shown += 1
        pct = count / total_failed * 100 if total_failed else 0
        marker = "  " if already else "* " if likely_bad else "  "
        print(f"{rank:>4}  {count:>8,}  {pct:>4.1f}%  {marker}{domain:<43}  {note}")

        if not already:
            new_candidates.append((domain, count, likely_bad))

    # ── 未登録ドメインの要確認候補（上位） ──────────────────
    new_likely = [(d, c) for d, c, b in new_candidates if b]
    if new_likely:
        print("\n" + "=" * 90)
        print("【要確認】SKIP_DOMAINS 未登録かつ企業情報DB系の可能性があるドメイン:\n")
        for d, c in new_likely[:20]:
            print(f"  {d:<50} ({c:,}件)")

    # ── SKIP_DOMAINS 追加コード ───────────────────────────────
    print("\n" + "=" * 90)
    print("# SKIP_DOMAINS に追加するPythonコード（上位未登録ドメイン・要レビュー）:\n")
    unlisted_top = [(d, c) for d, c, _ in new_candidates[:30]]
    if unlisted_top:
        print("SKIP_DOMAINS_ADDITIONS = [")
        for d, c in unlisted_top:
            print(f'    "{d}",  # {c:,}件')
        print("]")
    else:
        print("  （上位に新規候補なし）")

    # ── ドメイン別エラー内訳 ─────────────────────────────────
    print("\n" + "=" * 90)
    print("【エラー内訳】url_failed の error フィールド集計:\n")
    conn2 = sqlite3.connect(os.path.expanduser(DB_PATH), timeout=30)
    err_rows = conn2.execute(
        "SELECT error, COUNT(*) as cnt FROM crawl_queue "
        "WHERE status='url_failed' GROUP BY error ORDER BY cnt DESC LIMIT 20"
    ).fetchall()
    conn2.close()
    for err, cnt in err_rows:
        print(f"  {cnt:>8,}  {err or '(NULL)'}")


if __name__ == "__main__":
    main()
