#!/usr/bin/env python3
"""
NTA法人データ 検索・フィルタUI
使い方: python3 ~/Downloads/search_houjin.py
ブラウザで http://localhost:5001 を開く
"""
import sqlite3
import csv
import io
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, quote

DB_PATH = os.path.expanduser('~/Downloads/houjin.db')

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NTA法人データ検索</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, sans-serif; background: #f5f5f5; color: #333; }
header { background: #1a73e8; color: white; padding: 16px 24px; }
header h1 { font-size: 20px; }
header small { opacity: 0.8; font-size: 13px; }
.container { max-width: 1400px; margin: 0 auto; padding: 20px; }
.filters { background: white; border-radius: 8px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }
.filters h2 { font-size: 15px; margin-bottom: 12px; color: #555; }
.filter-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; }
.filter-grid label { font-size: 12px; color: #777; display: block; margin-bottom: 4px; }
.filter-grid input, .filter-grid select {
  width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px;
}
.buttons { display: flex; gap: 10px; margin-top: 16px; }
.btn { padding: 9px 20px; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; }
.btn-primary { background: #1a73e8; color: white; }
.btn-secondary { background: #f1f3f4; color: #333; }
.btn-green { background: #34a853; color: white; }
.results { background: white; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); overflow: hidden; }
.results-header { padding: 16px 20px; border-bottom: 1px solid #eee; display: flex; align-items: center; justify-content: space-between; }
.results-header h3 { font-size: 15px; }
.count { color: #1a73e8; font-weight: 600; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: #f8f9fa; padding: 10px 12px; text-align: left; border-bottom: 2px solid #e0e0e0; font-size: 12px; color: #555; white-space: nowrap; position: sticky; top: 0; }
td { padding: 9px 12px; border-bottom: 1px solid #f0f0f0; vertical-align: top; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
tr:hover td { background: #f8f9fa; }
a { color: #1a73e8; text-decoration: none; }
a:hover { text-decoration: underline; }
.pagination { padding: 16px 20px; display: flex; gap: 8px; align-items: center; justify-content: center; border-top: 1px solid #eee; }
.pagination a { padding: 6px 12px; border: 1px solid #ddd; border-radius: 4px; color: #333; font-size: 13px; }
.pagination a.active { background: #1a73e8; color: white; border-color: #1a73e8; }
.pagination a:hover:not(.active) { background: #f5f5f5; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 500; }
.badge-done { background: #e6f4ea; color: #1e8e3e; }
.badge-error { background: #fce8e6; color: #d93025; }
.no-results { padding: 40px; text-align: center; color: #999; }
</style>
</head>
<body>
<header>
  <h1>NTA法人データ検索</h1>
  <small>国税庁 法人番号公表サイト データ ({total_count:,}件)</small>
</header>
<div class="container">
  <div class="filters">
    <h2>検索・フィルタ</h2>
    <form method="get" action="/">
      <div class="filter-grid">
        <div>
          <label>法人名（部分一致）</label>
          <input type="text" name="name" value="{q_name}" placeholder="例: 株式会社">
        </div>
        <div>
          <label>都道府県</label>
          <select name="pref">
            <option value="">-- すべて --</option>
            {pref_options}
          </select>
        </div>
        <div>
          <label>市区町村（部分一致）</label>
          <input type="text" name="city" value="{q_city}" placeholder="例: 渋谷区">
        </div>
        <div>
          <label>法人種別</label>
          <select name="kind">
            <option value="">-- すべて --</option>
            {kind_options}
          </select>
        </div>
        <div>
          <label>HP URL（部分一致）</label>
          <input type="text" name="url" value="{q_url}" placeholder="例: .co.jp">
        </div>
        <div>
          <label>ステータス</label>
          <select name="status">
            <option value="has_url" {sel_has_url}>HP URLあり</option>
            <option value="done" {sel_done}>done（スクレイプ成功）</option>
            <option value="error" {sel_error}>error（URLのみ）</option>
            <option value="all" {sel_all}>すべて</option>
          </select>
        </div>
      </div>
      <div class="buttons">
        <button type="submit" class="btn btn-primary">検索</button>
        <a href="/" class="btn btn-secondary">リセット</a>
        <a href="/export?{query_string}" class="btn btn-green">CSVダウンロード（フィルタ後）</a>
      </div>
    </form>
  </div>

  <div class="results">
    <div class="results-header">
      <h3>検索結果</h3>
      <span class="count">{result_count:,}件</span>
    </div>
    {table_html}
    {pagination_html}
  </div>
</div>
</body>
</html>"""


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def build_where(params):
    conditions = []
    args = []

    status = params.get('status', ['has_url'])[0]
    if status == 'has_url':
        conditions.append("q.status IN ('done','error') AND q.hp_url IS NOT NULL")
    elif status == 'done':
        conditions.append("q.status = 'done'")
    elif status == 'error':
        conditions.append("q.status = 'error' AND q.hp_url IS NOT NULL")

    name = params.get('name', [''])[0].strip()
    if name:
        conditions.append("c.name LIKE ?")
        args.append(f'%{name}%')

    pref = params.get('pref', [''])[0].strip()
    if pref:
        conditions.append("c.pref_name = ?")
        args.append(pref)

    city = params.get('city', [''])[0].strip()
    if city:
        conditions.append("c.city_name LIKE ?")
        args.append(f'%{city}%')

    kind = params.get('kind', [''])[0].strip()
    if kind:
        conditions.append("c.kind = ?")
        args.append(kind)

    url = params.get('url', [''])[0].strip()
    if url:
        conditions.append("q.hp_url LIKE ?")
        args.append(f'%{url}%')

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    return where, args


def get_options(conn, column, table, selected):
    rows = conn.execute(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column}").fetchall()
    opts = ''
    for r in rows:
        v = r[0]
        sel = 'selected' if v == selected else ''
        opts += f'<option value="{v}" {sel}>{v}</option>'
    return opts


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == '/export':
            self.handle_export(params)
        else:
            self.handle_search(params)

    def handle_search(self, params):
        conn = get_conn()
        page = int(params.get('page', ['1'])[0])
        per_page = 50

        where, args = build_where(params)
        query = f"""
            SELECT q.corporate_number, c.name, c.furigana, c.kind,
                   c.pref_name, c.city_name, c.street_number, c.post_code,
                   q.hp_url, c.hp_title, c.hp_phone, c.hp_email, q.status
            FROM crawl_queue q
            LEFT JOIN corporations c ON q.corporate_number = c.corporate_number
            {where}
            ORDER BY q.corporate_number
            LIMIT ? OFFSET ?
        """
        count_query = f"""
            SELECT COUNT(*) FROM crawl_queue q
            LEFT JOIN corporations c ON q.corporate_number = c.corporate_number
            {where}
        """
        total_count = conn.execute("SELECT COUNT(*) FROM crawl_queue").fetchone()[0]
        result_count = conn.execute(count_query, args).fetchone()[0]
        rows = conn.execute(query, args + [per_page, (page-1)*per_page]).fetchall()

        q_name = params.get('name', [''])[0]
        q_city = params.get('city', [''])[0]
        q_url = params.get('url', [''])[0]
        q_pref = params.get('pref', [''])[0]
        q_kind = params.get('kind', [''])[0]
        q_status = params.get('status', ['has_url'])[0]

        pref_opts = get_options(conn, 'pref_name', 'corporations', q_pref)
        kind_opts = get_options(conn, 'kind', 'corporations', q_kind)

        # Build table
        if rows:
            table = '<div style="overflow-x:auto"><table><thead><tr>'
            headers = ['法人番号','法人名','フリガナ','法人種別','都道府県','市区町村','番地','郵便番号','HP URL','HPタイトル','電話番号','メール','状態']
            for h in headers:
                table += f'<th>{h}</th>'
            table += '</tr></thead><tbody>'
            for r in rows:
                url_cell = f'<a href="{r["hp_url"]}" target="_blank">{r["hp_url"]}</a>' if r["hp_url"] else ''
                badge_cls = 'badge-done' if r['status'] == 'done' else 'badge-error'
                table += f'''<tr>
                    <td>{r["corporate_number"] or ""}</td>
                    <td>{r["name"] or ""}</td>
                    <td>{r["furigana"] or ""}</td>
                    <td>{r["kind"] or ""}</td>
                    <td>{r["pref_name"] or ""}</td>
                    <td>{r["city_name"] or ""}</td>
                    <td>{r["street_number"] or ""}</td>
                    <td>{r["post_code"] or ""}</td>
                    <td style="max-width:250px">{url_cell}</td>
                    <td>{r["hp_title"] or ""}</td>
                    <td>{r["hp_phone"] or ""}</td>
                    <td>{r["hp_email"] or ""}</td>
                    <td><span class="badge {badge_cls}">{r["status"]}</span></td>
                </tr>'''
            table += '</tbody></table></div>'
        else:
            table = '<div class="no-results">該当データがありません</div>'

        # Pagination
        total_pages = (result_count + per_page - 1) // per_page
        base_params = {k: v[0] for k, v in params.items() if k != 'page'}
        qs_base = '&'.join(f'{k}={quote(str(v))}' for k, v in base_params.items())

        pagination = ''
        if total_pages > 1:
            pagination = '<div class="pagination">'
            start = max(1, page-3)
            end = min(total_pages, page+3)
            if start > 1:
                pagination += f'<a href="/?{qs_base}&page=1">1</a> ...'
            for p in range(start, end+1):
                cls = 'active' if p == page else ''
                pagination += f'<a href="/?{qs_base}&page={p}" class="{cls}">{p}</a>'
            if end < total_pages:
                pagination += f'... <a href="/?{qs_base}&page={total_pages}">{total_pages}</a>'
            pagination += '</div>'

        query_string = '&'.join(f'{k}={quote(str(v[0]))}' for k, v in params.items() if k != 'page')

        html = HTML_TEMPLATE.format(
            total_count=total_count,
            result_count=result_count,
            q_name=q_name, q_city=q_city, q_url=q_url,
            pref_options=pref_opts, kind_options=kind_opts,
            sel_has_url='selected' if q_status=='has_url' else '',
            sel_done='selected' if q_status=='done' else '',
            sel_error='selected' if q_status=='error' else '',
            sel_all='selected' if q_status=='all' else '',
            table_html=table,
            pagination_html=pagination,
            query_string=query_string,
        )
        conn.close()

        body = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_export(self, params):
        conn = sqlite3.connect(DB_PATH)
        where, args = build_where(params)
        query = f"""
            SELECT q.corporate_number, c.name, c.furigana, c.kind,
                   c.pref_name, c.city_name, c.street_number, c.post_code,
                   q.hp_url, c.hp_title, c.hp_description, c.hp_phone, c.hp_email,
                   q.status, c.hp_scraped_at
            FROM crawl_queue q
            LEFT JOIN corporations c ON q.corporate_number = c.corporate_number
            {where}
            ORDER BY q.corporate_number
        """
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(['法人番号','法人名','フリガナ','法人種別','都道府県','市区町村','番地','郵便番号',
                         'HP_URL','HPタイトル','HP概要','電話番号','メール','ステータス','スクレイプ日時'])
        cursor = conn.execute(query, args)
        while True:
            rows = cursor.fetchmany(5000)
            if not rows:
                break
            writer.writerows(rows)
        conn.close()

        body = ('﻿' + buf.getvalue()).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv; charset=utf-8')
        self.send_header('Content-Disposition', 'attachment; filename="houjin_filtered.csv"')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    import webbrowser
    port = 5001
    print(f'NTA法人データ検索サーバー起動中...')
    print(f'ブラウザで開いてください: http://localhost:{port}')
    print('終了するには Ctrl+C を押してください')
    server = HTTPServer(('localhost', port), Handler)
    webbrowser.open(f'http://localhost:{port}')
    server.serve_forever()
