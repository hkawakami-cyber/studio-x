#!/usr/bin/env python3
import sqlite3
import csv
import os

DB_PATH = os.path.expanduser('~/Downloads/houjin.db')
OUTPUT_PATH = os.path.expanduser('~/Downloads/houjin_export.csv')

conn = sqlite3.connect(DB_PATH)

print('=== NTA法人データ エクスポート ===')
counts = dict(conn.execute("SELECT status, COUNT(*) FROM crawl_queue GROUP BY status").fetchall())
print(f"done: {counts.get('done', 0):,}件")
print(f"error: {counts.get('error', 0):,}件")
print(f"url_failed: {counts.get('url_failed', 0):,}件")

total = conn.execute(
    "SELECT COUNT(*) FROM crawl_queue WHERE status IN ('done','error') AND hp_url IS NOT NULL"
).fetchone()[0]
print(f'エクスポート対象: {total:,}件')
print(f'出力先: {OUTPUT_PATH}')

query = """
SELECT
    q.corporate_number,
    c.name,
    c.furigana,
    c.kind,
    c.pref_name,
    c.city_name,
    c.street_number,
    c.post_code,
    q.hp_url,
    c.hp_title,
    c.hp_description,
    c.hp_phone,
    c.hp_email,
    q.status,
    c.hp_scraped_at
FROM crawl_queue q
LEFT JOIN corporations c ON q.corporate_number = c.corporate_number
WHERE q.status IN ('done', 'error')
  AND q.hp_url IS NOT NULL
ORDER BY q.corporate_number
"""

with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.writer(f)
    writer.writerow([
        '法人番号', '法人名', 'フリガナ', '法人種別',
        '都道府県', '市区町村', '番地', '郵便番号',
        'HP_URL', 'HPタイトル', 'HP概要', '電話番号', 'メール',
        'ステータス', 'スクレイプ日時'
    ])
    count = 0
    cursor = conn.execute(query)
    while True:
        rows = cursor.fetchmany(10000)
        if not rows:
            break
        writer.writerows(rows)
        count += len(rows)
        print(f'  {count:,} / {total:,}件...', end='\r')

print(f'\n完了: {count:,}件')
size_mb = os.path.getsize(OUTPUT_PATH) / 1024 / 1024
print(f'ファイルサイズ: {size_mb:.1f} MB')
conn.close()
