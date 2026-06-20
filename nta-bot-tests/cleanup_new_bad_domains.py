#!/usr/bin/env python3
"""
新規不良ドメイン クリーンアップスクリプト
2026-06-20 スクリーニングで発見した ~48万件の不良ドメインを除去する

実行前に watchdog / enricher を停止してから実行すること:
  pkill -f "enricher.py"; pkill -f "watchdog.py"; sleep 2

実行方法:
  python3 ~/Downloads/nta-bot/tools/nta/bot/cleanup_new_bad_domains.py

完了後に watchdog を再起動:
  nohup python3 ~/Downloads/nta-bot/tools/nta/bot/watchdog.py > ~/Downloads/watchdog.log 2>&1 &
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path.home() / "Downloads" / "nta-bot" / "db" / "nta.db"

# スクリーニング（2026-06-20）で発見した不良ドメイン
# 推定クリーンアップ件数 ~48万件
NEW_BAD_DOMAINS = [
    "yayoi-kk.co.jp",              # 78K: 弥生会計サービス
    "nabutan.com",                  # 64K: 企業情報DB
    "companydata.tsujigawa.com",    # 47K: 企業情報DB
    "navitime.co.jp",               # 44K: 地図サービス
    "kanji.jitenon.jp",             # 42K: 漢字辞典
    "kaisharesearch.com",           # 32K: 企業情報DB
    "houjin.info",                  # 法人情報DB
    "houjin.jp",                    # 法人情報DB
    "houjin.goo.to",                # 法人情報DB
    "companyinformation.jp",        # 企業情報DB
    "compalyze.co.jp",              # 企業情報DB
    "korps.jp",                     # 企業情報DB
    "tsukulink.net",                # 企業情報DB
    "kawasaki-connect.jp",          # 企業情報DB
    "alarmbox.jp",                  # 企業情報DB
    "web.suke-dachi.jp",            # 企業情報DB
    "toukibo.ai-con.lawyer",        # 登記情報DB（21K）
    "weblio.jp",                    # 辞書サービス（26K）
    "ejje.weblio.jp",               # 辞書サービス
    "myoji-yurai.net",              # 名字由来サービス
    "mhlw.go.jp",                   # 厚労省（16K）
    # 市区町村公式サイト（企業HP ではない）
    "city.minato.tokyo.jp",
    "city.yokohama.lg.jp",
    "city.sapporo.jp",
    # 外国サイト・汎用サービス
    "reddit.com",                   # 16K
    "zhihu.com",
    "office.com",                   # Microsoft（8K）
    "athome.co.jp",                 # 不動産
    "mens-aso.co.jp",               # アパレル
]


def cleanup(db_path: Path):
    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")

    print(f"DB: {db_path}")
    print(f"対象ドメイン数: {len(NEW_BAD_DOMAINS)}")
    print()

    # 現在の状態確認
    before_done = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='done'"
    ).fetchone()[0]
    print(f"[前] done: {before_done:,}")

    # Step 1: corporations テーブルのクリア
    print("\n[Step 1] corporations のクリア中...")
    total_corp = 0
    for domain in NEW_BAD_DOMAINS:
        n = conn.execute(
            "UPDATE corporations SET hp_url=NULL, hp_title=NULL, hp_scraped_at=NULL "
            "WHERE hp_url LIKE ?",
            (f"%{domain}%",),
        ).rowcount
        if n > 0:
            print(f"  {domain}: {n:,} 件クリア")
        total_corp += n
    conn.commit()
    print(f"  → corporations 合計: {total_corp:,} 件")

    # Step 2: crawl_queue を pending にリセット
    print("\n[Step 2] crawl_queue を pending にリセット中...")
    total_queue = 0
    for domain in NEW_BAD_DOMAINS:
        n = conn.execute(
            "UPDATE crawl_queue SET status='pending', attempts=0, hp_url=NULL, error=NULL "
            "WHERE hp_url LIKE ? "
            "AND status IN ('done','url_found','url_failed','error','scraping','skip')",
            (f"%{domain}%",),
        ).rowcount
        if n > 0:
            print(f"  {domain}: {n:,} 件 → pending")
        total_queue += n
    conn.commit()
    print(f"  → crawl_queue 合計: {total_queue:,} 件")

    # 完了後の状態確認
    after = dict(conn.execute(
        "SELECT status, COUNT(*) FROM crawl_queue GROUP BY status"
    ).fetchall())
    print("\n[完了後のDB状態]")
    for status in ["done", "pending", "skip", "url_failed", "url_found",
                   "url_searching", "scraping", "error"]:
        print(f"  {status}: {after.get(status, 0):,}")

    conn.close()
    print(f"\n完了: corporations {total_corp:,} 件, crawl_queue {total_queue:,} 件 をリセットしました")
    print("\n次: watchdog を再起動してください")
    print("  nohup python3 ~/Downloads/nta-bot/tools/nta/bot/watchdog.py "
          "> ~/Downloads/watchdog.log 2>&1 &")


if __name__ == "__main__":
    cleanup(DB_PATH)
