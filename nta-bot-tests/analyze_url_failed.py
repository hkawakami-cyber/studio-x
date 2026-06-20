#!/usr/bin/env python3
"""
url_failed ドメイン分析 & triage スクリプト
Mac 上の houjin.db で実行:

  # ドメイン分析のみ（読み取り専用）
  python3 analyze_url_failed.py ~/Downloads/houjin.db

  # triage ドライラン（何件移動するか確認）
  python3 analyze_url_failed.py ~/Downloads/houjin.db --triage

  # triage 本番実行（DB を更新）
  python3 analyze_url_failed.py ~/Downloads/houjin.db --triage --apply

triage アクション:
  not_found (404)  → skip          (再試行不要、永久に存在しない)
  bad_url (CDN等)  → pending       (hp_url クリア、URL再検索)
  blocked (403)    → pending       (後でリトライ)
  no_url           → pending       (hp_url クリア、URL再検索)
  fetch_failed     → 変更なし      (watchdog の自動skip が担当)
"""

import os
import re
import sqlite3
import sys
from collections import Counter
from urllib.parse import urlparse

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "~/Downloads/houjin.db"
TOP_N   = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--top"), "50"))
DO_TRIAGE = "--triage" in sys.argv
DO_APPLY  = "--apply" in sys.argv

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

PATTERNS_LIKELY_BAD = re.compile(
    r"houjin|kaisha|hojin|corp|company|city\.|\.lg\.jp$|\.go\.jp$"
    r"|wikipedia|amazon|rakuten|yahoo|google|bing|indeed|doda|rikunabi|mynavi",
    re.IGNORECASE,
)

# triage アクション定義
TRIAGE_ACTIONS = {
    "not_found": ("skip",    "not_found_permanent", "skip    (404 → 永久消滅)"),
    "bad_url":   ("pending", None,                  "pending (CDN/画像 → URL再検索)"),
    "blocked":   ("pending", None,                  "pending (403 → 後でリトライ)"),
    "no_url":    ("pending", None,                  "pending (URL未発見 → URL再検索)"),
}


def extract_domain(url: str) -> str | None:
    try:
        return urlparse(url).netloc.removeprefix("www.") or None
    except Exception:
        return None


def is_skipped(domain: str) -> bool:
    return any(
        domain == d or domain.endswith("." + d) or d.endswith("." + domain)
        for d in SKIP_DOMAINS
    )


def run_triage(conn, apply: bool) -> dict[str, int]:
    label = "本番実行" if apply else "ドライラン"
    print(f"\n{'=' * 70}")
    print(f"  triage {label}")
    print(f"{'=' * 70}")

    counts = {}
    for error_type, (new_status, new_error, desc) in TRIAGE_ACTIONS.items():
        if new_status == "skip":
            sql_count = (
                "SELECT COUNT(*) FROM crawl_queue "
                "WHERE status='url_failed' AND error=?"
            )
            sql_update = (
                "UPDATE crawl_queue SET status='skip', error=? "
                "WHERE status='url_failed' AND error=?"
            )
        else:
            sql_count = (
                "SELECT COUNT(*) FROM crawl_queue "
                "WHERE status='url_failed' AND error=?"
            )
            sql_update = (
                "UPDATE crawl_queue SET status='pending', hp_url=NULL, attempts=0, error=NULL "
                "WHERE status='url_failed' AND error=?"
            )

        n = conn.execute(sql_count, (error_type,)).fetchone()[0]
        counts[error_type] = n

        action_str = f"→ {desc}"
        print(f"  {error_type:<12} {n:>8,}件  {action_str}")

        if apply and n > 0:
            if new_status == "skip":
                conn.execute(sql_update, (new_error, error_type))
            else:
                conn.execute(sql_update, (error_type,))

    if apply:
        conn.commit()
        print(f"\n  完了: {sum(counts.values()):,}件 を更新しました")
    else:
        total = sum(counts.values())
        print(f"\n  ドライラン: {total:,}件 が移動対象です")
        print("  本番実行: --apply オプションを付けてください")
    return counts


def main():
    path = os.path.expanduser(DB_PATH)
    print(f"\n接続: {path}")
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # ── 全体件数 ─────────────────────────────────────────────
    stats = dict(conn.execute(
        "SELECT status, COUNT(*) FROM crawl_queue GROUP BY status"
    ).fetchall())
    total_failed = stats.get("url_failed", 0)
    print(f"\nurl_failed: {total_failed:,}件  "
          f"error: {stats.get('error', 0):,}件  "
          f"skip: {stats.get('skip', 0):,}件  "
          f"done: {stats.get('done', 0):,}件\n")

    # ── エラー種別内訳 ───────────────────────────────────────
    print("▼ url_failed エラー種別内訳（triage アクション付き）\n")
    err_rows = conn.execute(
        "SELECT error, COUNT(*) as cnt FROM crawl_queue "
        "WHERE status='url_failed' GROUP BY error ORDER BY cnt DESC LIMIT 20"
    ).fetchall()

    for err, cnt in err_rows:
        pct = cnt / total_failed * 100 if total_failed else 0
        action = TRIAGE_ACTIONS.get(err or "", ("変更なし", None, "変更なし (watchdog 担当)"))[2]
        print(f"  {cnt:>8,}  {pct:>4.1f}%  {(err or '(NULL)'):<15}  → {action}")

    # ── ドメイン別集計（url_failed） ────────────────────────
    rows = conn.execute(
        "SELECT hp_url FROM crawl_queue WHERE status='url_failed' AND hp_url IS NOT NULL"
    ).fetchall()

    domains = [extract_domain(r[0]) for r in rows if r[0]]
    domains = [d for d in domains if d]
    counter = Counter(domains)

    if counter:
        print(f"\n▼ url_failed ドメイン上位 {TOP_N} 件\n")
        print(f"  {'rank':>4}  {'件数':>8}  {'%':>5}  ドメイン")
        print("  " + "-" * 65)

        new_candidates = []
        for rank, (domain, count) in enumerate(counter.most_common(TOP_N), 1):
            already = is_skipped(domain)
            likely_bad = bool(PATTERNS_LIKELY_BAD.search(domain))
            pct = count / total_failed * 100 if total_failed else 0
            tag = "★SKIP済" if already else ("?要確認" if likely_bad else "      ")
            print(f"  {rank:>4}  {count:>8,}  {pct:>4.1f}%  {domain:<45} {tag}")
            if not already:
                new_candidates.append((domain, count, likely_bad))

        # SKIP_DOMAINS 追加候補コード出力
        likely_new = [(d, c) for d, c, b in new_candidates if b]
        if likely_new:
            print(f"\n▼ 企業情報DB系の可能性あり（要確認・上位 20 件）\n")
            print("  SKIP_DOMAINS_ADDITIONS = [")
            for d, c in likely_new[:20]:
                print(f'      "{d}",  # {c:,}件')
            print("  ]")

    # ── triage 実行 ──────────────────────────────────────────
    if DO_TRIAGE:
        run_triage(conn, apply=DO_APPLY)

    conn.close()
    print()


if __name__ == "__main__":
    main()
