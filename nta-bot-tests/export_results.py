#!/usr/bin/env python3
"""
クロール結果を CSV にエクスポートする。
別PC上の全国570万社DBと法人番号で突合する用途を想定。
再調査に備えてフルデータ（所在地・エラー種別・試行回数）を含む。

使い方:
  python3 export_results.py                              # デフォルトDB・全ステータス
  python3 export_results.py ~/Downloads/nta-bot/data/crawl.db
  python3 export_results.py ~/path/to/crawl.db -o export.csv
  python3 export_results.py ~/path/to/crawl.db --done-only   # done のみ
  python3 export_results.py ~/path/to/crawl.db --status done,skip

出力カラム:
  corporate_number  法人番号（突合キー）
  name              企業名
  kind              法人種別
  pref_name         都道府県
  city_name         市区町村
  hp_url            確定HP URL (corporations テーブル優先、なければ crawl_queue)
  hp_title          スクレイプ取得タイトル
  hp_scraped_at     スクレイプ日時
  crawl_status      クロールステータス (done/skip/error/url_failed/pending 等)
  crawl_error       エラー種別
  crawl_attempts    試行回数
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path.home() / "Downloads/nta-bot/data/crawl.db"
COLUMNS = [
    "corporate_number",
    "name",
    "kind",
    "pref_name",
    "city_name",
    "hp_url",
    "hp_title",
    "hp_scraped_at",
    "crawl_status",
    "crawl_error",
    "crawl_attempts",
]

QUERY = """
SELECT
    q.corporate_number,
    q.name,
    c.kind,
    q.pref_name,
    q.city_name,
    COALESCE(c.hp_url, q.hp_url)  AS hp_url,
    c.hp_title                     AS hp_title,
    c.hp_scraped_at                AS hp_scraped_at,
    q.status                       AS crawl_status,
    q.error                        AS crawl_error,
    q.attempts                     AS crawl_attempts
FROM crawl_queue q
LEFT JOIN corporations c USING (corporate_number)
{where}
ORDER BY q.corporate_number
"""


def main():
    parser = argparse.ArgumentParser(description="nta-bot クロール結果 CSV エクスポート")
    parser.add_argument("db", nargs="?", default=str(DEFAULT_DB),
                        help=f"SQLite DB パス (デフォルト: {DEFAULT_DB})")
    parser.add_argument("-o", "--output", default=None,
                        help="出力 CSV ファイルパス (デフォルト: stdout)")
    parser.add_argument("--done-only", action="store_true",
                        help="status='done' のレコードのみ出力")
    parser.add_argument("--status", default=None,
                        help="カンマ区切りで出力するステータスを指定 (例: done,skip)")

    args = parser.parse_args()

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        print(f"ERROR: DB が見つかりません: {db_path}", file=sys.stderr)
        sys.exit(1)

    # WHERE 句の構築
    if args.done_only:
        where = "WHERE q.status = 'done'"
    elif args.status:
        statuses = [s.strip() for s in args.status.split(",") if s.strip()]
        placeholders = ",".join("?" * len(statuses))
        where = f"WHERE q.status IN ({placeholders})"
    else:
        where = ""

    query = QUERY.format(where=where)
    params = []
    if args.status and not args.done_only:
        params = [s.strip() for s in args.status.split(",") if s.strip()]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute(query, params)

    if args.output:
        out_path = Path(args.output).expanduser()
        f = open(out_path, "w", newline="", encoding="utf-8-sig")
        close_file = True
    else:
        f = sys.stdout
        close_file = False

    try:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        count = 0
        for row in cursor:
            writer.writerow(dict(row))
            count += 1
    finally:
        if close_file:
            f.close()

    conn.close()

    if args.output:
        print(f"エクスポート完了: {count:,} 件 → {args.output}", file=sys.stderr)
    else:
        print(f"\n# エクスポート: {count:,} 件", file=sys.stderr)


if __name__ == "__main__":
    main()
