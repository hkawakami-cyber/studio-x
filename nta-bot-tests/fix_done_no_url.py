#!/usr/bin/env python3
"""
status='done' なのに hp_url が NULL のレコードを診断・修正する。

実行前に watchdog を停止:
  pkill -f watchdog.py; sleep 2

使い方:
  # 診断のみ（読み取り専用）
  python3 fix_done_no_url.py ~/Downloads/houjin.db

  # 修正ドライラン（何件変更するか確認）
  python3 fix_done_no_url.py ~/Downloads/houjin.db --fix

  # 修正本番実行
  python3 fix_done_no_url.py ~/Downloads/houjin.db --fix --apply

修正方針:
  crawl_queue.hp_url IS NULL かつ corporations.hp_url IS NULL
    → crawl_queue.status を 'pending' にリセット（URL検索からやり直し）

  crawl_queue.hp_url IS NULL かつ corporations.hp_url IS NOT NULL
    → 整合性異常。crawl_queue.hp_url に corporations.hp_url をコピー（status はそのまま）

完了後に watchdog を再起動:
  nohup python3 ~/Downloads/nta-bot/tools/nta/bot/watchdog.py > ~/Downloads/watchdog.log 2>&1 &
"""

import sqlite3
import sys
from pathlib import Path

DB_DEFAULT = Path.home() / "Downloads" / "houjin.db"
DO_FIX   = "--fix"   in sys.argv
DO_APPLY = "--apply" in sys.argv

db_arg = next((a for a in sys.argv[1:] if not a.startswith("--")), None)
DB_PATH = Path(db_arg).expanduser() if db_arg else DB_DEFAULT


def main():
    if not DB_PATH.exists():
        print(f"ERROR: DB が見つかりません: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")

    print(f"\n接続: {DB_PATH}")
    print("=" * 70)

    # ── 全体サマリー ──────────────────────────────────────────────────
    stats = dict(conn.execute(
        "SELECT status, COUNT(*) FROM crawl_queue GROUP BY status"
    ).fetchall())
    total = sum(stats.values())
    print(f"\n▼ crawl_queue 全体 ({total:,} 件)")
    for s in ["done", "skip", "url_failed", "pending", "url_found",
              "url_searching", "scraping", "error"]:
        print(f"  {s:<15} {stats.get(s, 0):>10,}")

    # ── done & hp_url NULL の内訳 ─────────────────────────────────────
    print("\n▼ status='done' のURL空欄診断\n")

    # パターンA: crawl_queue.hp_url IS NULL
    n_q_null = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='done' AND hp_url IS NULL"
    ).fetchone()[0]

    # パターンB: crawl_queue.hp_url = ''
    n_q_empty = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='done' AND hp_url = ''"
    ).fetchone()[0]

    # パターンC: crawl_queue.hp_url IS NULL だが corporations.hp_url には値あり
    n_corp_has_url = conn.execute(
        """SELECT COUNT(*) FROM crawl_queue q
           JOIN corporations c USING (corporate_number)
           WHERE q.status='done' AND q.hp_url IS NULL AND c.hp_url IS NOT NULL"""
    ).fetchone()[0]

    # パターンD: 両方NULL（真の「URLなし done」）
    n_both_null = conn.execute(
        """SELECT COUNT(*) FROM crawl_queue q
           LEFT JOIN corporations c USING (corporate_number)
           WHERE q.status='done' AND q.hp_url IS NULL
             AND (c.hp_url IS NULL OR c.hp_url = '')"""
    ).fetchone()[0]

    print(f"  crawl_queue.hp_url IS NULL           : {n_q_null:>10,} 件")
    print(f"  crawl_queue.hp_url = '' (空文字)      : {n_q_empty:>10,} 件")
    print(f"    うち corporations.hp_url あり       : {n_corp_has_url:>10,} 件  ← 整合性異常")
    print(f"    うち corporations.hp_url も NULL    : {n_both_null:>10,} 件  ← 再クロール対象")

    # サンプル表示
    print("\n  [サンプル: crawl_queue.hp_url IS NULL の先頭5件]")
    rows = conn.execute(
        """SELECT q.corporate_number, q.name, q.pref_name,
                  q.attempts, q.error, c.hp_url AS corp_hp_url
           FROM crawl_queue q
           LEFT JOIN corporations c USING (corporate_number)
           WHERE q.status='done' AND q.hp_url IS NULL
           LIMIT 5"""
    ).fetchall()
    for r in rows:
        corp_url = r[5] or "(NULL)"
        print(f"    {r[0]}  {r[1][:20]:<20}  {r[2]:<5}  "
              f"attempts={r[3]}  error={r[4]}  corp.hp_url={corp_url[:40]}")

    if not DO_FIX:
        print("\n診断のみ完了（--fix を付けると修正を実行）")
        conn.close()
        return

    # ── 修正 ──────────────────────────────────────────────────────────
    label = "本番実行" if DO_APPLY else "ドライラン"
    print(f"\n{'=' * 70}")
    print(f"  修正 [{label}]")
    print(f"{'=' * 70}\n")

    if DO_APPLY:
        # Step 1: 整合性異常（crawl_queue.hp_url=NULL だが corp に URL あり）→ URL コピー
        n1 = conn.execute(
            """UPDATE crawl_queue SET hp_url = (
                   SELECT hp_url FROM corporations c
                   WHERE c.corporate_number = crawl_queue.corporate_number
               )
               WHERE status='done' AND hp_url IS NULL
                 AND EXISTS (
                   SELECT 1 FROM corporations c
                   WHERE c.corporate_number = crawl_queue.corporate_number
                     AND c.hp_url IS NOT NULL AND c.hp_url != ''
                 )"""
        ).rowcount
        conn.commit()
        print(f"  [Step 1] 整合性修正 (corp→queue URL コピー): {n1:,} 件")

        # Step 2: 両方 NULL → pending にリセット
        n2 = conn.execute(
            """UPDATE crawl_queue
               SET status='pending', attempts=0, error=NULL
               WHERE status='done' AND hp_url IS NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM corporations c
                   WHERE c.corporate_number = crawl_queue.corporate_number
                     AND c.hp_url IS NOT NULL AND c.hp_url != ''
                 )"""
        ).rowcount
        conn.commit()
        print(f"  [Step 2] 両方NULL → pending リセット       : {n2:,} 件")

        # 空文字もリセット
        n3 = conn.execute(
            "UPDATE crawl_queue SET hp_url=NULL, status='pending', attempts=0, error=NULL "
            "WHERE status='done' AND hp_url = ''"
        ).rowcount
        conn.commit()
        print(f"  [Step 3] hp_url='' → pending リセット      : {n3:,} 件")

        print(f"\n  完了: 合計 {n1 + n2 + n3:,} 件を修正しました")

    else:
        print(f"  [Step 1] 整合性修正 (corp→queue URL コピー): {n_corp_has_url:,} 件 (予定)")
        print(f"  [Step 2] 両方NULL → pending リセット       : {n_both_null:,} 件 (予定)")
        print(f"  [Step 3] hp_url='' → pending リセット      : {n_q_empty:,} 件 (予定)")
        print(f"\n  合計 {n_corp_has_url + n_both_null + n_q_empty:,} 件が修正対象です")
        print("  本番実行: --apply オプションを付けてください")

    # 完了後のサマリー
    if DO_APPLY:
        after = dict(conn.execute(
            "SELECT status, COUNT(*) FROM crawl_queue GROUP BY status"
        ).fetchall())
        total_after = sum(after.values())
        print(f"\n▼ 修正後の crawl_queue ({total_after:,} 件)")
        for s in ["done", "skip", "url_failed", "pending", "url_found",
                  "url_searching", "scraping", "error"]:
            print(f"  {s:<15} {after.get(s, 0):>10,}")
        finished = after.get("done", 0) + after.get("skip", 0) + after.get("url_failed", 0)
        pct = finished / total_after * 100 if total_after else 0
        print(f"\n  進捗: {pct:.1f}% (done+skip+url_failed = {finished:,} / {total_after:,})")

    conn.close()

    if DO_APPLY:
        print("\n次: watchdog を再起動してください")
        print("  nohup python3 ~/Downloads/nta-bot/tools/nta/bot/watchdog.py "
              "> ~/Downloads/watchdog.log 2>&1 &")


if __name__ == "__main__":
    main()
