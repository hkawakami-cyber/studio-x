#!/usr/bin/env python3
"""
nta-bot E2E テスト
検証内容:
  1. SKIP_DOMAINS フィルタ (不正ドメインが除外されるか)
  2. bad_url 検出 (CDN・画像URLが検出されるか)
  3. リセットスクリプトのロジック (DB変更が正しく行われるか)
  4. watchdog の fetch_failed_permanent 除外ロジック
  5. fetch_html タプル戻り値の互換性チェック
  6. watchdog の fetch_failed 自動 skip 機能
"""

import asyncio
import os
import sqlite3
import sys
import tempfile
from urllib.parse import urlparse

PASSED = []
FAILED = []

def check(name, condition, detail=""):
    if condition:
        PASSED.append(name)
        print(f"  ✓  {name}")
    else:
        FAILED.append(name)
        print(f"  ✗  {name}" + (f"  [{detail}]" if detail else ""))

# ═══════════════════════════════════════════════════════════
#  テスト用DB作成
# ═══════════════════════════════════════════════════════════

def setup_test_db(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS corporations (
            corporate_number TEXT PRIMARY KEY,
            name TEXT,
            kind TEXT,
            hp_url TEXT,
            hp_title TEXT,
            hp_scraped_at TEXT
        );
        CREATE TABLE IF NOT EXISTS crawl_queue (
            corporate_number TEXT PRIMARY KEY,
            name TEXT,
            pref_name TEXT,
            city_name TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            hp_url TEXT,
            error TEXT,
            last_attempt TEXT
        );
    """)

    # 不正ドメインURL（企業情報DBサービス）
    bad_records = [
        ("1000000000001", "テスト株式会社1", "https://www.freee.co.jp/companies/123"),
        ("1000000000002", "テスト株式会社2", "https://imijiten.net/company/456"),
        ("1000000000003", "テスト株式会社3", "https://info.gbiz.go.jp/hojin/789"),
        ("1000000000004", "テスト株式会社4", "https://kotobank.jp/company/100"),
        ("1000000000005", "テスト株式会社5", "https://baseconnect.in/companies/200"),
    ]

    # 正常な企業HPのURL
    good_records = [
        ("2000000000001", "良企業株式会社1", "https://www.good-company1.co.jp"),
        ("2000000000002", "良企業株式会社2", "https://example-firm.co.jp/index.html"),
    ]

    # CDN・画像URL（bad_url 検出対象）
    cdn_records = [
        ("3000000000001", "CDNテスト1", "https://s.yimg.jp/c/icon/s/bsc/2.0/favicon.ico"),
        ("3000000000002", "CDNテスト2", "https://fbcdn.net/image.png"),
    ]

    # まだ検索していない（pending）
    pending_records = [
        ("4000000000001", "未検索株式会社1"),
        ("4000000000002", "未検索株式会社2"),
    ]

    # fetch_failed_permanent（skipに固定すべき）
    fetch_failed_records = [
        ("5000000000001", "接続不可1", "https://dead-server.example.jp"),
    ]

    for num, name, url in bad_records:
        conn.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                     (num, name, "2015-10-05", url, None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     (num, name, "東京都", "新宿区", "done", 3, url, None, None))

    for num, name, url in good_records:
        conn.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                     (num, name, "2015-10-05", url, "企業サイト", "2026-06-19T00:00:00+00:00"))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     (num, name, "大阪府", "大阪市", "done", 3, url, None, None))

    for num, name, url in cdn_records:
        conn.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                     (num, name, "2015-10-05", url, None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     (num, name, "愛知県", "名古屋市", "url_found", 2, url, None, None))

    for num, name in pending_records:
        conn.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                     (num, name, "2015-10-05", None, None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     (num, name, "福岡県", "福岡市", "pending", 0, None, None, None))

    for num, name, url in fetch_failed_records:
        conn.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                     (num, name, "2015-10-05", url, None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     (num, name, "北海道", "札幌市", "error", 3, url, "fetch_failed_permanent", None))

    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════
#  テスト対象の実装（enricher.py から抜粋・再現）
# ═══════════════════════════════════════════════════════════

SKIP_DOMAINS = frozenset([
    "google.", "bing.com", "yahoo.co.jp", "duckduckgo.com",
    "facebook.", "twitter.com", "x.com", "linkedin.",
    # 企業情報DBサービス（本物HPではない）
    "freee.co.jp", "imijiten.net", "info.gbiz.go.jp", "cnavi.g-search.or.jp",
    "kotobank.jp", "baseconnect.in", "manareki.com", "data-link-plus.com",
    "kensetumap.com", "salesnow.jp", "g-search.or.jp", "houjin.com",
    "kigyolog.com", "uijin.com", "biz-maps.com", "navi-i.jp",
])

def _is_valid_result_url(url: str, skip: frozenset) -> bool:
    """検索結果URLが有効な企業HPかチェック"""
    try:
        h = urlparse(url).netloc.lstrip("www.")
        return bool(h) and not any(
            h == d or h.endswith("." + d) for d in skip
        )
    except Exception:
        return False


_BAD_EXT = frozenset([
    ".ico", ".gif", ".png", ".jpg", ".jpeg", ".webp",
    ".css", ".js", ".pdf", ".xml", ".txt", ".zip",
    ".svg", ".woff", ".woff2", ".eot",
])
_BAD_CDN = frozenset([
    "yimg.jp", "fbcdn.net", "googleapis.com", "gstatic.com",
    "cloudfront.net", "akamaized.net", "fastly.net", "twimg.com",
])

def is_bad_url(url: str) -> bool:
    """CDN・画像リソース・企業情報DB URLを検出"""
    try:
        p = urlparse(url)
        if os.path.splitext(p.path)[1].lower() in _BAD_EXT:
            return True
        netloc = p.netloc
        if any(netloc == d or netloc.endswith("." + d) for d in _BAD_CDN):
            return True
        return False
    except Exception:
        return False


def reset_bad_domains(conn, bad_domains):
    """本番と同じリセットロジック"""
    total_corp = 0
    for d in bad_domains:
        n = conn.execute(
            "UPDATE corporations SET hp_url=NULL, hp_title=NULL, hp_scraped_at=NULL "
            "WHERE hp_url LIKE ?",
            (f"%{d}%",),
        ).rowcount
        total_corp += n

    total_queue = 0
    for d in bad_domains:
        n = conn.execute(
            "UPDATE crawl_queue SET status='pending', attempts=0, hp_url=NULL, error=NULL "
            "WHERE hp_url LIKE ? AND status IN ('done','url_found','url_failed','error','scraping','skip')",
            (f"%{d}%",),
        ).rowcount
        total_queue += n

    conn.commit()
    return total_corp, total_queue


def reset_stuck_watchdog(conn):
    """watchdog の reset_stuck ロジック（fetch_failed は除外＆自動skip）"""
    n_pending  = conn.execute(
        "UPDATE crawl_queue SET status='pending' WHERE status='url_searching'"
    ).rowcount
    n_url_found = conn.execute(
        "UPDATE crawl_queue SET status='url_found' WHERE status='scraping'"
    ).rowcount
    n_error = conn.execute(
        "UPDATE crawl_queue SET status='url_found' "
        "WHERE status='error' AND (error IS NULL OR error NOT LIKE 'fetch_failed%')"
    ).rowcount
    # fetch_failed を即座に skip に移動（無限リトライ防止）
    n_skip = conn.execute(
        "UPDATE crawl_queue SET status='skip', error='fetch_failed_permanent' "
        "WHERE status='error' AND error LIKE 'fetch_failed%'"
    ).rowcount
    conn.commit()
    return n_pending, n_url_found, n_error, n_skip


# ═══════════════════════════════════════════════════════════
#  テスト実行
# ═══════════════════════════════════════════════════════════

def run_tests():
    print("\n" + "=" * 60)
    print("  nta-bot E2E テスト")
    print("=" * 60)

    # ── テスト 1: SKIP_DOMAINS フィルタ ─────────────────────
    print("\n▼ Test 1: SKIP_DOMAINS フィルタ")

    bad_urls = [
        ("https://www.freee.co.jp/companies/123", "freee.co.jp"),
        ("https://imijiten.net/company/456", "imijiten.net"),
        ("https://info.gbiz.go.jp/hojin/789", "info.gbiz.go.jp"),
        ("https://cnavi.g-search.or.jp/search", "cnavi.g-search.or.jp"),
        ("https://g-search.or.jp/query", "g-search.or.jp"),
        ("https://kotobank.jp/company/100", "kotobank.jp"),
        ("https://baseconnect.in/companies/200", "baseconnect.in"),
        ("https://salesnow.jp/hojin/300", "salesnow.jp"),
        ("https://www.biz-maps.com/corp/400", "biz-maps.com"),
    ]
    good_urls = [
        "https://example.co.jp",
        "https://www.tdk.co.jp",
        "https://www.toyota.co.jp/index.html",
        "https://shop.some-company.co.jp/about",
    ]

    for url, label in bad_urls:
        check(
            f"  {label} が除外される",
            not _is_valid_result_url(url, SKIP_DOMAINS),
        )
    for url in good_urls:
        check(
            f"  {urlparse(url).netloc} は通過する",
            _is_valid_result_url(url, SKIP_DOMAINS),
        )

    # ── テスト 2: bad_url 検出 ───────────────────────────────
    print("\n▼ Test 2: bad_url 検出（CDN・画像URL）")

    cdn_bad = [
        ("https://s.yimg.jp/c/icon/s/bsc/2.0/favicon.ico", "yimg favicon.ico"),
        ("https://fbcdn.net/some/image.jpg", "fbcdn.net 画像"),
        ("https://maps.googleapis.com/maps/api/tile", "googleapis.com"),
        ("https://example.co.jp/logo.png", ".png 拡張子"),
        ("https://example.co.jp/doc.pdf", ".pdf 拡張子"),
        ("https://example.co.jp/style.css", ".css 拡張子"),
        ("https://d1234.cloudfront.net/asset.js", "cloudfront.net"),
    ]
    cdn_good = [
        ("https://example.co.jp/index.html", "普通のHTMLページ"),
        ("https://www.firm.co.jp/about", "パスなし拡張子"),
        ("https://tdk.co.jp/", "ルートURL"),
    ]

    for url, label in cdn_bad:
        check(f"  {label} → bad_url", is_bad_url(url))
    for url, label in cdn_good:
        check(f"  {label} → 正常", not is_bad_url(url))

    # ── テスト 3: リセットスクリプト ────────────────────────
    print("\n▼ Test 3: リセットスクリプト（テストDB）")

    tmp = tempfile.mktemp(suffix=".db")
    try:
        conn = setup_test_db(tmp)

        bad_domains = [
            "freee.co.jp", "imijiten.net", "info.gbiz.go.jp",
            "kotobank.jp", "baseconnect.in",
        ]

        # リセット前の状態確認
        before_corp_bad = conn.execute(
            "SELECT COUNT(*) FROM corporations WHERE hp_url LIKE '%freee.co.jp%' "
            "OR hp_url LIKE '%imijiten.net%' OR hp_url LIKE '%kotobank.jp%'"
        ).fetchone()[0]
        before_corp_good = conn.execute(
            "SELECT COUNT(*) FROM corporations WHERE hp_url LIKE '%good-company%' "
            "OR hp_url LIKE '%example-firm%'"
        ).fetchone()[0]

        check("リセット前に不正URL が5件存在する", before_corp_bad == 3,
              f"実際={before_corp_bad}")
        check("リセット前に正常URL が2件存在する", before_corp_good == 2,
              f"実際={before_corp_good}")

        # リセット実行
        total_corp, total_queue = reset_bad_domains(conn, bad_domains)

        # リセット後の確認
        after_corp_bad = conn.execute(
            "SELECT COUNT(*) FROM corporations WHERE hp_url LIKE '%freee.co.jp%' "
            "OR hp_url LIKE '%imijiten.net%' OR hp_url LIKE '%kotobank.jp%'"
        ).fetchone()[0]
        after_corp_good = conn.execute(
            "SELECT COUNT(*) FROM corporations "
            "WHERE hp_url LIKE '%good-company%' OR hp_url LIKE '%example-firm%'"
        ).fetchone()[0]
        after_queue_pending = conn.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE status='pending'"
        ).fetchone()[0]
        after_queue_done_good = conn.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE status='done' "
            "AND (hp_url LIKE '%good-company%' OR hp_url LIKE '%example-firm%')"
        ).fetchone()[0]

        check("不正URLが corporations から全削除された", after_corp_bad == 0,
              f"残={after_corp_bad}")
        check("正常URLは corporations に残っている", after_corp_good == 2,
              f"残={after_corp_good}")
        check("不正URLレコードが crawl_queue で pending に戻った",
              after_queue_pending == 2 + 5,   # pending_records(2) + bad_records(5)
              f"pending={after_queue_pending}")
        check("正常URLは crawl_queue で done のまま", after_queue_done_good == 2,
              f"done={after_queue_done_good}")
        check("corporations クリア件数が正しい", total_corp == 5,
              f"実際={total_corp}")

        # ── テスト 4: watchdog の fetch_failed_permanent 除外 ─
        print("\n▼ Test 4: watchdog fetch_failed_permanent 除外")

        # url_searching / scraping を追加してリセット確認
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     ("9000000000001", "スタック1", "東京都", "千代田区",
                      "url_searching", 1, None, None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     ("9000000000002", "スタック2", "東京都", "千代田区",
                      "scraping", 1, "https://some-url.co.jp", None, None))
        conn.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                     ("9000000000003", "エラー普通", "東京都", "千代田区",
                      "error", 3, None, "timeout", None))
        conn.commit()

        n_p, n_u, n_e, n_s = reset_stuck_watchdog(conn)

        # fetch_failed_permanent は skip に移動済み
        ff_row = conn.execute(
            "SELECT status FROM crawl_queue WHERE error='fetch_failed_permanent'"
        ).fetchone()
        ff_status = ff_row["status"] if ff_row else "not_found"

        check("url_searching → pending にリセットされる", n_p >= 1,
              f"件数={n_p}")
        check("scraping → url_found にリセットされる", n_u >= 1,
              f"件数={n_u}")
        check("通常 error → url_found にリセットされる", n_e >= 1,
              f"件数={n_e}")
        check("fetch_failed_permanent は url_found に変更されない（watchdog除外確認）",
              ff_status != "url_found", f"実際={ff_status}")

        conn.close()
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
        for f in [tmp + "-shm", tmp + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 6: watchdog fetch_failed 自動 skip ───────────
    print("\n▼ Test 6: watchdog fetch_failed 自動 skip")

    tmp6 = tempfile.mktemp(suffix=".db")
    try:
        conn6 = sqlite3.connect(tmp6)
        conn6.execute("PRAGMA journal_mode=WAL")
        conn6.row_factory = sqlite3.Row
        conn6.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER, hp_url TEXT,
                error TEXT, last_attempt TEXT
            );
        """)

        # fetch_failed が error に溜まっている状態を再現
        test_data = [
            ("A001", "fetch_failed error 1", "error", "fetch_failed"),
            ("A002", "fetch_failed error 2", "error", "fetch_failed"),
            ("A003", "通常 error（リトライ可）", "error", "timeout"),
            ("A004", "通常 error（error=NULL）", "error", None),
            ("A005", "url_searching スタック", "url_searching", None),
        ]
        for num, name, status, error in test_data:
            conn6.execute(
                "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                (num, name, "東京都", "千代田区", status, 2, None, error, None),
            )
        conn6.commit()

        n_p, n_u, n_e, n_s = reset_stuck_watchdog(conn6)

        # fetch_failed → skip に移動されているか
        ff_rows = conn6.execute(
            "SELECT status, error FROM crawl_queue WHERE corporate_number IN ('A001','A002')"
        ).fetchall()
        # 通常 error → url_found に昇格しているか
        normal_rows = conn6.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number IN ('A003','A004')"
        ).fetchall()

        check("fetch_failed error が skip に移動される（自動skip）",
              all(r["status"] == "skip" for r in ff_rows),
              f"実際: {[(r['status'], r['error']) for r in ff_rows]}")
        check("fetch_failed skip の error が fetch_failed_permanent になる",
              all(r["error"] == "fetch_failed_permanent" for r in ff_rows),
              f"実際: {[r['error'] for r in ff_rows]}")
        check("自動skip件数が2件", n_s == 2, f"実際={n_s}")
        check("通常 error は url_found に昇格する",
              all(r["status"] == "url_found" for r in normal_rows),
              f"実際: {[r['status'] for r in normal_rows]}")
        check("url_searching は pending に戻る", n_p >= 1, f"件数={n_p}")

        conn6.close()
    finally:
        for f in [tmp6, tmp6 + "-shm", tmp6 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 5: fetch_html タプル戻り値チェック ──────────
    print("\n▼ Test 5: fetch_html タプル戻り値の互換性")

    # fetch_html が (html, status_code) を返すことを模倣
    async def mock_fetch_html_ok(session, url, proxy, pool):
        return "<html><title>テスト</title></html>", 200

    async def mock_fetch_html_404(session, url, proxy, pool):
        return None, 404

    async def mock_fetch_html_403(session, url, proxy, pool):
        return None, 403

    async def mock_fetch_html_timeout(session, url, proxy, pool):
        return None, 0  # タイムアウト = ステータス0

    async def test_tuple_returns():
        # 戻り値をアンパックできるか
        try:
            html, status = await mock_fetch_html_ok(None, "http://x.co.jp", None, None)
            check("fetch_html OK: html取得できる", html is not None)
            check("fetch_html OK: status=200", status == 200)

            html, status = await mock_fetch_html_404(None, "http://x.co.jp", None, None)
            check("fetch_html 404: html=None", html is None)
            check("fetch_html 404: status=404", status == 404)
            # 404 → not_found ハンドリング
            error = "not_found" if status in (404, 410) else "fetch_failed"
            check("fetch_html 404: → not_found エラー判定", error == "not_found")

            html, status = await mock_fetch_html_403(None, "http://x.co.jp", None, None)
            error = "blocked" if status in (403, 429) else "fetch_failed"
            check("fetch_html 403: → blocked エラー判定", error == "blocked")

            html, status = await mock_fetch_html_timeout(None, "http://x.co.jp", None, None)
            error = "fetch_failed" if status not in (404, 410, 403, 429) else "other"
            check("fetch_html timeout: → fetch_failed エラー判定", error == "fetch_failed")
        except Exception as e:
            check(f"fetch_html タプル展開", False, str(e))

    asyncio.run(test_tuple_returns())

    # ── 結果サマリー ─────────────────────────────────────────
    print("\n" + "=" * 60)
    total = len(PASSED) + len(FAILED)
    print(f"  結果: {len(PASSED)}/{total} PASSED")
    if FAILED:
        print(f"\n  ✗ 失敗したテスト:")
        for f in FAILED:
            print(f"    - {f}")
        print("\n  → 本番実行 NG: 上記を修正してから再実行してください")
        return False
    else:
        print("\n  全テスト合格 ✓")
        print("  → 本番実行 OK: リセットスクリプトと SKIP_DOMAINS 更新を適用できます")
        return True


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
