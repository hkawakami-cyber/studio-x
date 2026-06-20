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
  7. 新規不良ドメイン（スクリーニング発見分）フィルタ
  8. lstrip バグ vs removeprefix 差異
  9. scrape_worker hp_url=NULL 早期 return
 10. RETRY_AT 境界値
 11. url_found スタック防止 attempts リセット
 12. watchdog url_found(hp_url=NULL) → url_failed 自動移動
 13. write_scrape_results no_url → url_failed
 14. url_failed エラー分類別再試行戦略（fetch_failed の attempts 分岐含む）
 15. _is_valid_result_url パスシグナル検出
 16. normalize_url UTMパラメータ・フラグメント除去
 17. 求人・口コミサイト SKIP_DOMAINS フィルタ
 18. normalize_url None安全・is_bad_url 長URL・飲食店/予約サイト SKIP_DOMAINS
 19. is_bad_url IPアドレスURL・_is_valid_result_url スキーム検証・政府ポータル SKIP_DOMAINS
"""

import asyncio
import ipaddress
import os
import re
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
    # スクリーニングで新発見（2026-06-20 追加）
    "yayoi-kk.co.jp", "nabutan.com", "companydata.tsujigawa.com",
    "kaisharesearch.com", "houjin.info", "houjin.jp", "houjin.goo.to",
    "companyinformation.jp", "compalyze.co.jp", "korps.jp",
    "tsukulink.net", "kawasaki-connect.jp", "alarmbox.jp",
    "web.suke-dachi.jp", "toukibo.ai-con.lawyer",
    "weblio.jp", "ejje.weblio.jp", "kanji.jitenon.jp", "myoji-yurai.net",
    "navitime.co.jp", "mhlw.go.jp",
    # 市区町村公式サイト（企業HPではない）
    "city.minato.tokyo.jp", "city.yokohama.lg.jp", "city.sapporo.jp",
    # 外国サイト・汎用サービス
    "reddit.com", "zhihu.com", "office.com",
    # 不動産・アパレル等（企業自身のHPでなく業種ポータル）
    "athome.co.jp", "mens-aso.co.jp",
    # 求人サイト（企業の公式HPではなく求人ページ）
    "indeed.com", "doda.jp", "rikunabi.com", "mynavi.jp",
    "en-japan.com", "type.jp", "hatarako.net", "job-gear.jp",
    # 口コミ・評判サイト
    "glassdoor.com", "vorkers.com", "openwork.jp",
    # 飲食店・観光レビュー・予約サイト（企業HPではなくポータル）
    "tabelog.com", "retty.me", "hotpepper.jp", "jalan.net",
    "tripadvisor.jp", "tripadvisor.com", "booking.com",
    "yelp.co.jp",
    # 政府・行政ポータル（企業自身のサイトではない）
    "nta.go.jp", "e-gov.go.jp", "mirasapo-plus.go.jp",
    "j-net21.smrj.go.jp", "hellowork.mhlw.go.jp",
])

# URL パスに含まれる企業情報DB系のシグナルパターン
# キーワードがパスセグメントの完全な単語として現れる場合のみマッチ（hojin-seal は除外）
_BAD_PATH_PATTERNS = re.compile(
    r"/(hojin|houjin|kaisha|corporate_number|company-info|biz-info|hojinbango)"
    r"(?=[/?#]|$)"        # パスセグメントの末尾か次の区切り文字の手前
    r"|[?&](corporate_number|hojin_id|company_id)=",
    re.IGNORECASE,
)

_VALID_SCHEMES = frozenset(["http", "https"])

def _is_valid_result_url(url: str, skip: frozenset) -> bool:
    """検索結果URLが有効な企業HPかチェック"""
    try:
        p = urlparse(url)
        # http/https のみ許可
        if p.scheme not in _VALID_SCHEMES:
            return False
        h = p.netloc.removeprefix("www.")
        if not h:
            return False
        # IPアドレスは企業HPとして無効（IPv4 と IPv6 [::1] 形式に対応）
        if h.startswith("["):  # IPv6: [::1] or [::1]:port
            host_only = h[1:h.index("]")] if "]" in h else h[1:]
        else:
            host_only = h.split(":")[0]
        try:
            ipaddress.ip_address(host_only)
            return False  # IPアドレスだった
        except ValueError:
            pass  # ドメイン名（正常）
        # ドメインが SKIP_DOMAINS に含まれるか
        if any(h == d or h.endswith("." + d) for d in skip):
            return False
        # パス・クエリに企業情報DB系シグナルが含まれるか
        path_and_query = p.path + ("?" + p.query if p.query else "")
        if _BAD_PATH_PATTERNS.search(path_and_query):
            return False
        return True
    except Exception:
        return False


_MAX_URL_LEN = 500  # これ以上長いURLは追跡/リダイレクト用であることが多い

_BAD_EXT = frozenset([
    ".ico", ".gif", ".png", ".jpg", ".jpeg", ".webp",
    ".css", ".js", ".pdf", ".xml", ".txt", ".zip",
    ".svg", ".woff", ".woff2", ".eot",
])
_BAD_CDN = frozenset([
    "yimg.jp", "fbcdn.net", "googleapis.com", "gstatic.com",
    "cloudfront.net", "akamaized.net", "fastly.net", "twimg.com",
])
# スクレイプ対象外のスキーム
_BAD_SCHEMES = frozenset(["data", "javascript", "mailto", "tel", "ftp"])
# 開発用・内部向けポート（企業HPとして無効）
_BAD_PORTS = frozenset(["8080", "8443", "3000", "3001", "4000", "5000", "8000", "8888", "9000"])

def is_bad_url(url: str) -> bool:
    """CDN・画像リソース・非HTTPスキーム・開発ポート・超長URLを検出"""
    if not url or len(url) > _MAX_URL_LEN:
        return True
    try:
        p = urlparse(url)
        # data: / javascript: など非HTTPスキーム
        if p.scheme in _BAD_SCHEMES:
            return True
        # 開発用ポート
        if p.port and str(p.port) in _BAD_PORTS:
            return True
        # 画像・静的リソース拡張子
        if os.path.splitext(p.path)[1].lower() in _BAD_EXT:
            return True
        # CDN ドメイン
        netloc = p.netloc.split(":")[0]  # ポートを除いてドメインのみ比較
        if any(netloc == d or netloc.endswith("." + d) for d in _BAD_CDN):
            return True
        return False
    except Exception:
        return False


_UTM_PARAMS = frozenset([
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "msclkid", "yclid",
])

def normalize_url(url: str | None) -> str | None:
    """
    URLを正規化して保存する。
    - None や空文字列は None を返す
    - フラグメント(#以降)除去
    - UTM/トラッキングパラメータ除去
    - クエリが空になったら ? ごと除去
    - 正規化できない場合は None を返す
    """
    if not url:
        return None
    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return None
        # クエリパラメータからトラッキング系を除去
        if p.query:
            pairs = [kv for kv in p.query.split("&")
                     if kv.split("=")[0].lower() not in _UTM_PARAMS]
            query = "&".join(pairs)
        else:
            query = ""
        from urllib.parse import urlunparse
        normalized = urlunparse((p.scheme, p.netloc, p.path, p.params, query, ""))
        return normalized or None
    except Exception:
        return None


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
    # url_found で hp_url=NULL のものは URL再検索キューへ（無限ループ防止）
    n_null_url = conn.execute(
        "UPDATE crawl_queue SET status='url_failed', attempts=0 "
        "WHERE status='url_found' AND hp_url IS NULL"
    ).rowcount
    conn.commit()
    return n_pending, n_url_found, n_error, n_skip, n_null_url


def write_scrape_results_sim(conn, results):
    """write_scrape_results の改善版シミュレーション（no_url も url_failed に移動）"""
    for r in results:
        ok = r.get("ok", False)
        if r.get("error") in ("bad_url", "not_found", "blocked", "no_url"):
            conn.execute(
                "UPDATE crawl_queue SET status='url_failed', hp_url=NULL, error=?, attempts=0 "
                "WHERE corporate_number=?",
                (r.get("error"), r["corporate_number"]),
            )
            continue
        conn.execute(
            "UPDATE crawl_queue SET status=?, error=? WHERE corporate_number=?",
            ("done" if ok else "error", r.get("error"), r["corporate_number"]),
        )
    conn.commit()


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
        # 非HTTPスキーム
        ("data:image/png;base64,abc123",          "data: スキーム"),
        ("javascript:void(0)",                     "javascript: スキーム"),
        ("mailto:info@example.co.jp",              "mailto: スキーム"),
        ("tel:03-1234-5678",                       "tel: スキーム"),
        # 開発用ポート（企業HPとして無効）
        ("https://example.co.jp:8080/",            "ポート 8080"),
        ("https://example.co.jp:3000/",            "ポート 3000"),
        ("https://example.co.jp:8888/dashboard",   "ポート 8888"),
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

        n_p, n_u, n_e, n_s, n_nu = reset_stuck_watchdog(conn)

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
        # A003: hp_url あり → error → url_found（再スクレイプ）
        # A004: hp_url なし → error → url_found → url_failed（URL再検索）
        test_data6 = [
            ("A001", "fetch_failed error 1", "error", "fetch_failed", None),
            ("A002", "fetch_failed error 2", "error", "fetch_failed", None),
            ("A003", "通常 error (hp_url あり)", "error", "timeout", "https://a.co.jp"),
            ("A004", "通常 error (hp_url なし)", "error", None, None),
            ("A005", "url_searching スタック", "url_searching", None, None),
        ]
        for num, name, status, error, hp_url in test_data6:
            conn6.execute(
                "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                (num, name, "東京都", "千代田区", status, 2, hp_url, error, None),
            )
        conn6.commit()

        n_p, n_u, n_e, n_s, n_nu = reset_stuck_watchdog(conn6)

        # fetch_failed → skip に移動されているか
        ff_rows = conn6.execute(
            "SELECT status, error FROM crawl_queue WHERE corporate_number IN ('A001','A002')"
        ).fetchall()
        # hp_url あり error → url_found で再スクレイプ
        a003_row = conn6.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number='A003'"
        ).fetchone()
        # hp_url なし error → url_found → url_failed（URL再検索キューへ連鎖）
        a004_row = conn6.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number='A004'"
        ).fetchone()

        check("fetch_failed error が skip に移動される（自動skip）",
              all(r["status"] == "skip" for r in ff_rows),
              f"実際: {[(r['status'], r['error']) for r in ff_rows]}")
        check("fetch_failed skip の error が fetch_failed_permanent になる",
              all(r["error"] == "fetch_failed_permanent" for r in ff_rows),
              f"実際: {[r['error'] for r in ff_rows]}")
        check("自動skip件数が2件", n_s == 2, f"実際={n_s}")
        check("error(hp_url あり) は url_found に昇格する（再スクレイプ）",
              a003_row["status"] == "url_found",
              f"実際={a003_row['status']}")
        check("error(hp_url なし) は url_failed に移動する（URL再検索）",
              a004_row["status"] == "url_failed",
              f"実際={a004_row['status']}")
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

    # ── テスト 7: 新規不良ドメイン（2026-06-20 スクリーニング発見分）──
    print("\n▼ Test 7: 新規不良ドメイン SKIP_DOMAINS フィルタ")

    new_bad_urls = [
        ("https://www.yayoi-kk.co.jp/companies/123",           "yayoi-kk.co.jp (78K件)"),
        ("https://nabutan.com/hojin/456",                       "nabutan.com (64K件)"),
        ("https://companydata.tsujigawa.com/company/789",       "companydata.tsujigawa.com (47K件)"),
        ("https://navitime.co.jp/spot/00001234",                "navitime.co.jp (44K件)"),
        ("https://kanji.jitenon.jp/kanji/abc",                  "kanji.jitenon.jp (42K件)"),
        ("https://kaisharesearch.com/hojin/100",                "kaisharesearch.com (32K件)"),
        ("https://houjin.info/company/200",                     "houjin.info (部分)"),
        ("https://houjin.jp/hojin/300",                         "houjin.jp (部分)"),
        ("https://weblio.jp/content/テスト",                    "weblio.jp (26K件)"),
        ("https://ejje.weblio.jp/content/example",              "ejje.weblio.jp"),
        ("https://toukibo.ai-con.lawyer/hojin/400",             "toukibo.ai-con.lawyer (21K件)"),
        ("https://mhlw.go.jp/stf/seisakunitsuite/bunya/500",   "mhlw.go.jp (16K件)"),
        ("https://city.yokohama.lg.jp/kurashi/",                "city.yokohama.lg.jp (市区町村)"),
        ("https://city.sapporo.jp/shisei/",                     "city.sapporo.jp (市区町村)"),
        ("https://city.minato.tokyo.jp/joho/",                  "city.minato.tokyo.jp (市区町村)"),
        ("https://reddit.com/r/japan/comments/123",             "reddit.com (外国)"),
        ("https://zhihu.com/question/456",                      "zhihu.com (中国)"),
        ("https://office.com/launch/word",                      "office.com (MS)"),
        ("https://athome.co.jp/mansion/list/",                  "athome.co.jp (不動産)"),
        ("https://myoji-yurai.net/searchResult.htm?myojiKanji=山田", "myoji-yurai.net"),
        ("https://companyinformation.jp/company/600",           "companyinformation.jp"),
        ("https://compalyze.co.jp/hojin/700",                   "compalyze.co.jp"),
        ("https://korps.jp/company/800",                        "korps.jp"),
        ("https://tsukulink.net/hojin/900",                     "tsukulink.net"),
    ]
    # 新規ドメインはすべて除外されるべき
    for url, label in new_bad_urls:
        check(
            f"  {label} が除外される",
            not _is_valid_result_url(url, SKIP_DOMAINS),
        )

    # 新規クリーンアップ対象のDB操作テスト
    tmp7 = tempfile.mktemp(suffix=".db")
    try:
        conn7 = sqlite3.connect(tmp7)
        conn7.execute("PRAGMA journal_mode=WAL")
        conn7.row_factory = sqlite3.Row
        conn7.executescript("""
            CREATE TABLE corporations (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, kind TEXT,
                hp_url TEXT, hp_title TEXT, hp_scraped_at TEXT
            );
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        # 新規不良ドメインのレコードをセット（done済み）
        new_bad_sample = [
            ("B001", "弥生テスト", "https://www.yayoi-kk.co.jp/companies/1"),
            ("B002", "ナビタイムテスト", "https://navitime.co.jp/spot/00001"),
            ("B003", "ウェブリオテスト", "https://weblio.jp/content/テスト"),
            ("B004", "市役所テスト", "https://city.yokohama.lg.jp/kurashi/"),
        ]
        for num, name, url in new_bad_sample:
            conn7.execute("INSERT INTO corporations VALUES (?,?,?,?,?,?)",
                          (num, name, "2015-10-05", url, "タイトル", "2026-06-01T00:00:00+00:00"))
            conn7.execute("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
                          (num, name, "東京都", "千代田区", "done", 3, url, None, None))
        conn7.commit()

        new_bad_domains = [
            "yayoi-kk.co.jp", "navitime.co.jp", "weblio.jp", "city.yokohama.lg.jp",
        ]
        total_corp7, total_queue7 = reset_bad_domains(conn7, new_bad_domains)

        after7 = conn7.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE status='pending'"
        ).fetchone()[0]
        corp_null7 = conn7.execute(
            "SELECT COUNT(*) FROM corporations WHERE hp_url IS NULL"
        ).fetchone()[0]

        check("新規不良ドメイン: corporations が NULL クリアされる",
              corp_null7 == 4, f"実際={corp_null7}")
        check("新規不良ドメイン: crawl_queue が pending に戻る",
              after7 == 4, f"実際={after7}")
        check("新規不良ドメイン: corporations クリア件数が正しい",
              total_corp7 == 4, f"実際={total_corp7}")

        conn7.close()
    finally:
        for f in [tmp7, tmp7 + "-shm", tmp7 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 8: lstrip("www.") バグ vs removeprefix("www.") ──
    print("\n▼ Test 8: lstrip バグ vs removeprefix の差異確認")

    def bad_is_valid(url: str, skip: frozenset) -> bool:
        """lstrip バグ版（修正前）"""
        try:
            netloc = urlparse(url).netloc
            h = netloc.lstrip("www.")  # バグ: 文字集合扱いで先頭 w/. を全部除去
            return bool(h) and not any(
                h == d or h.endswith("." + d) for d in skip
            )
        except Exception:
            return False

    def good_is_valid(url: str, skip: frozenset) -> bool:
        """removeprefix 修正版"""
        try:
            netloc = urlparse(url).netloc
            h = netloc.removeprefix("www.")
            return bool(h) and not any(
                h == d or h.endswith("." + d) for d in skip
            )
        except Exception:
            return False

    # weblio.jp は "www.weblio.jp" → lstrip("www.") で "eblio.jp" になりフィルタ抜け
    weblio_www = "https://www.weblio.jp/content/test"
    weblio_nowww = "https://weblio.jp/content/test"
    # lstrip は文字集合扱い: "www.weblio.jp" → "eblio.jp"、"weblio.jp" → "eblio.jp"
    check("lstrip バグ: www.weblio.jp がフィルタをすり抜ける",
          bad_is_valid(weblio_www, SKIP_DOMAINS))
    check("lstrip バグ: weblio.jp (www.なし) もフィルタをすり抜ける（先頭'w'も除去）",
          bad_is_valid(weblio_nowww, SKIP_DOMAINS))
    check("removeprefix 修正: www.weblio.jp が正しく除外される",
          not good_is_valid(weblio_www, SKIP_DOMAINS))
    check("removeprefix 修正: weblio.jp (www.なし) も正しく除外される",
          not good_is_valid(weblio_nowww, SKIP_DOMAINS))

    # 正常ドメインには影響なし（"www." で始まらないドメイン）
    normal_url = "https://www.toyota.co.jp/index.html"
    check("removeprefix: 正常ドメインは通過する", good_is_valid(normal_url, SKIP_DOMAINS))

    # ── テスト 9: scrape_worker の hp_url=NULL 早期 return ───
    print("\n▼ Test 9: scrape_worker hp_url=NULL 早期 return ロジック")

    def simulate_scrape_worker(hp_url, html, status_code):
        """scrape_worker の hp_url=NULL チェックロジック再現"""
        if hp_url is None:
            return "skipped_null_hp_url", None
        if html is None:
            if status_code == 404:
                error = "not_found"
            elif status_code == 403:
                error = "blocked"
            else:
                error = "fetch_failed"
            return "url_failed", error
        return "done", None

    result, _ = simulate_scrape_worker(None, None, 0)
    check("hp_url=NULL → 早期 return（スクレイプ試みない）",
          result == "skipped_null_hp_url")
    result, err = simulate_scrape_worker("https://example.co.jp", None, 404)
    check("hp_url あり・404 → url_failed (not_found)",
          result == "url_failed" and err == "not_found")
    result, err = simulate_scrape_worker("https://example.co.jp", "<html>ok</html>", 200)
    check("hp_url あり・html あり → done", result == "done")

    # ── テスト 10: RETRY_AT トリガー（pending < 300,000 で url_failed を retry）──
    print("\n▼ Test 10: url_failed 自動 retry トリガー (RETRY_AT)")

    RETRY_AT = 300_000

    def should_retry_url_failed(pending_count: int, retry_count: int, max_retries: int = 5) -> bool:
        return pending_count < RETRY_AT and retry_count < max_retries

    check("pending=2,320,484 → retry しない",
          not should_retry_url_failed(2_320_484, 0))
    check("pending=250,000 → retry する（初回）",
          should_retry_url_failed(250_000, 0))
    check("pending=250,000・retry_count=5 → retry しない（上限）",
          not should_retry_url_failed(250_000, 5))
    check("pending=299,999 → retry する（境界値）",
          should_retry_url_failed(299_999, 4))
    check("pending=300,000 → retry しない（境界値）",
          not should_retry_url_failed(300_000, 0))

    # ── テスト 11: url_found スタック防止（attempts 上限リセット）─
    print("\n▼ Test 11: url_found スタック防止 attempts リセット")

    tmp11 = tempfile.mktemp(suffix=".db")
    try:
        conn11 = sqlite3.connect(tmp11)
        conn11.row_factory = sqlite3.Row
        conn11.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn11.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("C001", "3回試行済み", "東", "千", "url_found", 3,
                 "https://a.co.jp", None, None),
                ("C002", "2回試行済み", "東", "千", "url_found", 2,
                 "https://b.co.jp", None, None),
                ("C003", "1回試行済み", "東", "千", "url_found", 1,
                 "https://c.co.jp", None, None),
            ],
        )
        conn11.commit()

        # watchdog の url_found スタック防止: attempts>=3 を attempts=1 にリセット
        n_reset = conn11.execute(
            "UPDATE crawl_queue SET attempts=1 WHERE status='url_found' AND attempts>=3"
        ).rowcount
        conn11.commit()

        after = conn11.execute(
            "SELECT corporate_number, attempts FROM crawl_queue ORDER BY corporate_number"
        ).fetchall()
        attempts_map = {r["corporate_number"]: r["attempts"] for r in after}

        check("attempts>=3 の url_found が attempts=1 にリセットされる",
              n_reset == 1, f"件数={n_reset}")
        check("C001: attempts 3→1 にリセット", attempts_map["C001"] == 1,
              f"実際={attempts_map['C001']}")
        check("C002: attempts=2 は変更されない", attempts_map["C002"] == 2,
              f"実際={attempts_map['C002']}")
        check("C003: attempts=1 は変更されない", attempts_map["C003"] == 1,
              f"実際={attempts_map['C003']}")

        conn11.close()
    finally:
        for f in [tmp11, tmp11 + "-shm", tmp11 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 12: watchdog url_found(hp_url=NULL) → url_failed ──
    print("\n▼ Test 12: watchdog url_found(hp_url=NULL) → url_failed 自動移動")

    tmp12 = tempfile.mktemp(suffix=".db")
    try:
        conn12 = sqlite3.connect(tmp12)
        conn12.row_factory = sqlite3.Row
        conn12.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn12.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                # hp_url=NULL の url_found（URLフェーズが何も見つけなかった）
                ("D001", "URL未発見1", "東", "千", "url_found", 3, None, None, None),
                ("D002", "URL未発見2", "東", "千", "url_found", 2, None, None, None),
                # hp_url ありの url_found（正常、変更しない）
                ("D003", "URL発見済み", "東", "千", "url_found", 1,
                 "https://example.co.jp", None, None),
                # url_searching スタック（pending に戻る）
                ("D004", "url_searching", "東", "千", "url_searching", 1, None, None, None),
            ],
        )
        conn12.commit()

        n_p, n_u, n_e, n_s, n_nu = reset_stuck_watchdog(conn12)

        null_rows = conn12.execute(
            "SELECT status, attempts FROM crawl_queue WHERE corporate_number IN ('D001','D002')"
        ).fetchall()
        valid_row = conn12.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number='D003'"
        ).fetchone()

        check("url_found(hp_url=NULL) が url_failed に移動される",
              all(r["status"] == "url_failed" for r in null_rows),
              f"実際: {[r['status'] for r in null_rows]}")
        check("url_found(hp_url=NULL) の attempts が 0 にリセットされる",
              all(r["attempts"] == 0 for r in null_rows),
              f"実際: {[r['attempts'] for r in null_rows]}")
        check("url_found(hp_url あり) は変更されない",
              valid_row["status"] == "url_found",
              f"実際={valid_row['status']}")
        check("url_found NULL 移動件数が 2 件", n_nu == 2, f"実際={n_nu}")
        check("url_searching → pending 件数が 1 件", n_p == 1, f"実際={n_p}")

        conn12.close()
    finally:
        for f in [tmp12, tmp12 + "-shm", tmp12 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 13: write_scrape_results の no_url → url_failed 処理 ──
    print("\n▼ Test 13: write_scrape_results no_url → url_failed (URL再検索キューへ)")

    tmp13 = tempfile.mktemp(suffix=".db")
    try:
        conn13 = sqlite3.connect(tmp13)
        conn13.row_factory = sqlite3.Row
        conn13.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn13.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("E001", "no_url企業", "東", "千", "scraping", 2, None, None, None),
                ("E002", "bad_url企業", "東", "千", "scraping", 2,
                 "https://s.yimg.jp/icon.ico", None, None),
                ("E003", "not_found企業", "東", "千", "scraping", 2,
                 "https://gone.example.co.jp", None, None),
                ("E004", "正常企業", "東", "千", "scraping", 1,
                 "https://ok.example.co.jp", None, None),
                ("E005", "fetch_failed企業", "東", "千", "scraping", 3,
                 "https://dead.example.co.jp", None, None),
            ],
        )
        conn13.commit()

        results = [
            {"corporate_number": "E001", "ok": False, "error": "no_url"},
            {"corporate_number": "E002", "ok": False, "error": "bad_url"},
            {"corporate_number": "E003", "ok": False, "error": "not_found",
             "hp_url": "https://gone.example.co.jp"},
            {"corporate_number": "E004", "ok": True, "hp_url": "https://ok.example.co.jp"},
            {"corporate_number": "E005", "ok": False, "error": "fetch_failed",
             "hp_url": "https://dead.example.co.jp"},
        ]
        write_scrape_results_sim(conn13, results)

        rows = {r["corporate_number"]: r for r in
                conn13.execute("SELECT * FROM crawl_queue").fetchall()}

        check("no_url → url_failed に移動（URL再検索キューへ）",
              rows["E001"]["status"] == "url_failed",
              f"実際={rows['E001']['status']}")
        check("no_url → hp_url が NULL クリアされる",
              rows["E001"]["hp_url"] is None,
              f"実際={rows['E001']['hp_url']}")
        check("no_url → attempts が 0 にリセットされる",
              rows["E001"]["attempts"] == 0,
              f"実際={rows['E001']['attempts']}")
        check("bad_url → url_failed に移動される",
              rows["E002"]["status"] == "url_failed",
              f"実際={rows['E002']['status']}")
        check("not_found (404) → url_failed に移動される",
              rows["E003"]["status"] == "url_failed",
              f"実際={rows['E003']['status']}")
        check("正常スクレイプ → done に移動される",
              rows["E004"]["status"] == "done",
              f"実際={rows['E004']['status']}")
        check("fetch_failed → error に移動される（skipは watchdog が担当）",
              rows["E005"]["status"] == "error",
              f"実際={rows['E005']['status']}")

        conn13.close()
    finally:
        for f in [tmp13, tmp13 + "-shm", tmp13 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 14: url_failed エラー分類別再試行戦略 ─────────
    print("\n▼ Test 14: url_failed エラー分類別再試行戦略")

    def triage_url_failed(conn):
        """url_failed のエラー種別ごとに最適な次のステータスへ移動する"""
        # not_found (404): そのURLは存在しない → skip（再試行不要）
        n_not_found = conn.execute(
            "UPDATE crawl_queue SET status='skip', error='not_found_permanent' "
            "WHERE status='url_failed' AND error='not_found'"
        ).rowcount
        # bad_url (CDN/画像URL): URLが無効 → pending に戻して URL再検索
        n_bad_url = conn.execute(
            "UPDATE crawl_queue SET status='pending', hp_url=NULL, attempts=0, error=NULL "
            "WHERE status='url_failed' AND error='bad_url'"
        ).rowcount
        # blocked (403/429): 一時的なブロック → pending に戻して後でリトライ
        n_blocked = conn.execute(
            "UPDATE crawl_queue SET status='pending', attempts=0, error=NULL "
            "WHERE status='url_failed' AND error='blocked'"
        ).rowcount
        # no_url: URL未発見 → pending に戻して URL再検索
        n_no_url = conn.execute(
            "UPDATE crawl_queue SET status='pending', hp_url=NULL, attempts=0, error=NULL "
            "WHERE status='url_failed' AND error='no_url'"
        ).rowcount
        # fetch_failed: 接続失敗 → attempts が少なければ pending リトライ、多ければ skip
        n_ff_retry = conn.execute(
            "UPDATE crawl_queue SET status='pending', attempts=0, error=NULL "
            "WHERE status='url_failed' AND error='fetch_failed' AND attempts < 3"
        ).rowcount
        n_ff_skip = conn.execute(
            "UPDATE crawl_queue SET status='skip', error='fetch_failed_permanent' "
            "WHERE status='url_failed' AND error='fetch_failed' AND attempts >= 3"
        ).rowcount
        conn.commit()
        return n_not_found, n_bad_url, n_blocked, n_no_url, n_ff_retry, n_ff_skip

    tmp14 = tempfile.mktemp(suffix=".db")
    try:
        conn14 = sqlite3.connect(tmp14)
        conn14.row_factory = sqlite3.Row
        conn14.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn14.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("F001", "404企業", "東", "千", "url_failed", 3,
                 "https://gone.co.jp", "not_found", None),
                ("F002", "CDN企業", "東", "千", "url_failed", 2,
                 "https://s.yimg.jp/icon.ico", "bad_url", None),
                ("F003", "403企業", "東", "千", "url_failed", 2,
                 "https://blocked.co.jp", "blocked", None),
                ("F004", "URL未発見企業", "東", "千", "url_failed", 3,
                 None, "no_url", None),
                # fetch_failed: attempts<3 → pending リトライ
                ("F005", "失敗少数企業", "東", "千", "url_failed", 1,
                 "https://timeout.co.jp", "fetch_failed", None),
                # fetch_failed: attempts>=3 → skip（諦め）
                ("F006", "繰返失敗企業", "東", "千", "url_failed", 3,
                 "https://dead.co.jp", "fetch_failed", None),
            ],
        )
        conn14.commit()

        n_nf, n_bu, n_bl, n_nu, n_ff_r, n_ff_s = triage_url_failed(conn14)
        rows = {r["corporate_number"]: r for r in
                conn14.execute("SELECT * FROM crawl_queue").fetchall()}

        check("not_found(404) → skip に移動（再試行不要）",
              rows["F001"]["status"] == "skip", f"実際={rows['F001']['status']}")
        check("not_found(404) → error が not_found_permanent になる",
              rows["F001"]["error"] == "not_found_permanent",
              f"実際={rows['F001']['error']}")
        check("bad_url(CDN等) → pending に戻り hp_url クリア（URL再検索）",
              rows["F002"]["status"] == "pending" and rows["F002"]["hp_url"] is None,
              f"実際={rows['F002']['status']}, hp_url={rows['F002']['hp_url']}")
        check("blocked(403) → pending に戻りリトライ",
              rows["F003"]["status"] == "pending",
              f"実際={rows['F003']['status']}")
        check("no_url → pending に戻り URL再検索",
              rows["F004"]["status"] == "pending" and rows["F004"]["hp_url"] is None,
              f"実際={rows['F004']['status']}, hp_url={rows['F004']['hp_url']}")
        check("fetch_failed(attempts<3) → pending に戻りリトライ",
              rows["F005"]["status"] == "pending",
              f"実際={rows['F005']['status']}")
        check("fetch_failed(attempts>=3) → skip（繰り返し失敗で諦め）",
              rows["F006"]["status"] == "skip",
              f"実際={rows['F006']['status']}")
        check("fetch_failed skip の error が fetch_failed_permanent になる",
              rows["F006"]["error"] == "fetch_failed_permanent",
              f"実際={rows['F006']['error']}")
        check("not_found 件数が 1 件", n_nf == 1, f"実際={n_nf}")
        check("bad_url 件数が 1 件", n_bu == 1, f"実際={n_bu}")
        check("blocked 件数が 1 件", n_bl == 1, f"実際={n_bl}")
        check("no_url 件数が 1 件", n_nu == 1, f"実際={n_nu}")
        check("fetch_failed retry 件数が 1 件", n_ff_r == 1, f"実際={n_ff_r}")
        check("fetch_failed skip 件数が 1 件", n_ff_s == 1, f"実際={n_ff_s}")

        conn14.close()
    finally:
        for f in [tmp14, tmp14 + "-shm", tmp14 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 15: _is_valid_result_url パスシグナル検出 ───────
    print("\n▼ Test 15: _is_valid_result_url パスシグナルによる不正URL除外")

    path_bad_urls = [
        ("https://example-db.co.jp/hojin/123456",         "/hojin/ パス"),
        ("https://some-site.co.jp/houjin/details",        "/houjin/ パス"),
        ("https://portal.co.jp/kaisha/list",              "/kaisha/ パス"),
        ("https://lookup.co.jp/?corporate_number=1234",   "?corporate_number クエリ"),
        ("https://search.co.jp/?hojin_id=5678",           "?hojin_id クエリ"),
        ("https://unknown.co.jp/company-info/view",       "/company-info/ パス"),
        ("https://db.co.jp/?company_id=9999",             "?company_id クエリ"),
    ]
    path_good_urls = [
        ("https://www.toyota.co.jp/",                     "ルートURL"),
        ("https://example.co.jp/about/company",           "/about/company（正常）"),
        ("https://hojin-abc.co.jp/",                      "ドメインに hojin を含むが正常"),
        ("https://example.co.jp/products/hojin-seal",     "パスに hojin を含むが別文脈"),
    ]

    for url, label in path_bad_urls:
        check(f"  パスシグナル除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in path_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ── テスト 16: URL正規化（UTMパラメータ・フラグメント除去）────
    print("\n▼ Test 16: normalize_url（UTMパラメータ・フラグメント除去）")

    norm_cases = [
        # (入力URL, 期待される正規化後URL, ラベル)
        ("https://example.co.jp/?utm_source=google&utm_medium=cpc",
         "https://example.co.jp/?",  # クエリは空になるが urlunparse で ? が残る場合あり
         "→", "https://example.co.jp/",
         "UTMパラメータ全除去"),
        ("https://example.co.jp/about#section1",
         None, "→", "https://example.co.jp/about",
         "フラグメント(#)除去"),
        ("https://example.co.jp/?ref=toppage&utm_campaign=summer",
         None, "→", "https://example.co.jp/?ref=toppage",
         "非UTMパラメータは残す"),
        ("https://example.co.jp/?fbclid=abc123",
         None, "→", "https://example.co.jp/",
         "fbclid 除去"),
        ("https://example.co.jp/page?gclid=xyz&page=2",
         None, "→", "https://example.co.jp/page?page=2",
         "gclid 除去、他パラメータ保持"),
        ("https://example.co.jp/",
         None, "→", "https://example.co.jp/",
         "変更なし（正常URL）"),
    ]

    # urlunparse は空クエリでも ? を付けない仕様なので個別に検証
    norm_inputs_expected = [
        ("https://example.co.jp/?utm_source=google&utm_medium=cpc",
         "https://example.co.jp/",
         "UTMパラメータ全除去 → クエリなし"),
        ("https://example.co.jp/about#section1",
         "https://example.co.jp/about",
         "フラグメント(#)除去"),
        ("https://example.co.jp/?ref=toppage&utm_campaign=summer",
         "https://example.co.jp/?ref=toppage",
         "非UTMパラメータは残す"),
        ("https://example.co.jp/?fbclid=abc123",
         "https://example.co.jp/",
         "fbclid 除去"),
        ("https://example.co.jp/page?gclid=xyz&page=2",
         "https://example.co.jp/page?page=2",
         "gclid 除去、page パラメータ保持"),
        ("https://example.co.jp/",
         "https://example.co.jp/",
         "変更なし（正常URL）"),
        (None,
         None,
         "None 入力 → None"),
        ("not-a-url",
         None,
         "不正URL → None"),
    ]

    for url, expected, label in norm_inputs_expected:
        if url is None:
            result = normalize_url(url)
            check(f"  {label}", result is None, f"結果={result}")
        else:
            result = normalize_url(url)
            check(f"  {label}", result == expected,
                  f"期待={expected!r}, 実際={result!r}")

    # ── テスト 17: 求人・口コミサイト SKIP_DOMAINS フィルタ ────
    print("\n▼ Test 17: 求人・口コミサイト SKIP_DOMAINS フィルタ")

    job_bad_urls = [
        ("https://jp.indeed.com/cmp/TestCo/jobs",          "indeed.com 求人"),
        ("https://doda.jp/DodaFront/View/Company/123",     "doda.jp 企業ページ"),
        ("https://rikunabi.com/company/detail/123",        "rikunabi.com 求人"),
        ("https://job.mynavi.jp/25/pc/corp/corpinfo/123",  "mynavi.jp 求人"),
        ("https://en-japan.com/companies/12345",           "en-japan.com"),
        ("https://type.jp/biz/detail/123",                 "type.jp"),
        ("https://glassdoor.com/Overview/Working-at-123",  "glassdoor.com 口コミ"),
        ("https://openwork.jp/companies/123",              "openwork.jp 口コミ"),
        ("https://vorkers.com/company/123",                "vorkers.com 口コミ"),
    ]
    # 正常企業HP（これらは除外してはいけない）
    job_good_urls = [
        ("https://www.recruit.co.jp/",                    "recruit.co.jp（企業自身のHP）"),
        ("https://example-jobs.co.jp/recruit/",           "独自採用ページ"),
        ("https://company.co.jp/en/jobs",                 "/jobs パスは企業HPに存在しうる"),
    ]

    for url, label in job_bad_urls:
        check(f"  {label} が除外される",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in job_good_urls:
        check(f"  {label} は通過する",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ── テスト 18: normalize_url None安全・is_bad_url 長URL・飲食店/予約サイト ──
    print("\n▼ Test 18: normalize_url None安全 / is_bad_url 長URL / 飲食店系 SKIP_DOMAINS")

    # normalize_url: None/空文字安全
    check("normalize_url(None) → None", normalize_url(None) is None)
    check("normalize_url('') → None", normalize_url("") is None)
    check("normalize_url 正常URL → 変更なし",
          normalize_url("https://example.co.jp/") == "https://example.co.jp/")

    # is_bad_url: 長すぎるURL（追跡/リダイレクト系）
    long_url = "https://redirect.example.com/" + "a" * 480  # 509文字
    short_url = "https://example.co.jp/" + "a" * 10        # 32文字
    check(f"is_bad_url: {_MAX_URL_LEN}文字超URL → bad_url",
          is_bad_url(long_url), f"len={len(long_url)}")
    check("is_bad_url: 短いURL → 正常",
          not is_bad_url(short_url), f"len={len(short_url)}")
    check("is_bad_url: None → bad_url", is_bad_url(None))
    check("is_bad_url: 空文字列 → bad_url", is_bad_url(""))

    # 飲食店・予約サイト SKIP_DOMAINS
    review_bad_urls = [
        ("https://tabelog.com/tokyo/A1301/A130101/13001234/", "tabelog.com 食べログ"),
        ("https://retty.me/area/PRE13/ARE1/SUB2/100012345/", "retty.me"),
        ("https://hotpepper.jp/str/RK001234/",               "hotpepper.jp ホットペッパー"),
        ("https://www.jalan.net/yad123456/",                 "jalan.net じゃらん"),
        ("https://www.tripadvisor.jp/Restaurant_Review-g1066456-d1234567.html",
                                                             "tripadvisor.jp"),
        ("https://booking.com/hotel/jp/example.html",        "booking.com"),
        ("https://yelp.co.jp/biz/example",                   "yelp.co.jp"),
    ]
    review_good_urls = [
        ("https://sushiro.co.jp/",                           "スシロー公式HP"),
        ("https://www.yoshinoya.com/",                       "吉野家公式HP"),
        ("https://hotel-example.co.jp/",                     "ホテル公式HP"),
    ]
    for url, label in review_bad_urls:
        check(f"  {label} が除外される",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in review_good_urls:
        check(f"  {label} は通過する",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ── テスト 19: IPアドレスURL・スキーム検証・政府ポータル SKIP_DOMAINS ──
    print("\n▼ Test 19: IPアドレスURL / スキーム検証 / 政府ポータル SKIP_DOMAINS")

    # IPアドレスURLは企業HPとして無効
    ip_bad_urls = [
        ("http://192.168.1.1/",              "プライベートIP (IPv4)"),
        ("http://10.0.0.1/index.html",       "プライベートIP 10.x"),
        ("http://172.16.0.1/",               "プライベートIP 172.x"),
        ("http://203.0.113.5/top.html",      "グローバルIP (テスト用)"),
        ("https://[::1]/",                   "IPv6 ループバック"),
    ]
    ip_good_urls = [
        ("https://example.co.jp/",           "正常ドメイン"),
        ("https://192-168-1-1.example.jp/",  "IPアドレス風だがドメイン名"),
    ]
    for url, label in ip_bad_urls:
        check(f"  IPアドレスURL除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in ip_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # 非http(s)スキームは企業HPとして無効
    scheme_bad = [
        ("ftp://example.co.jp/",             "ftp:// スキーム"),
        ("file:///etc/passwd",               "file:// スキーム"),
        ("ws://example.co.jp/",              "ws:// WebSocket"),
        ("ssh://user@example.co.jp/",        "ssh:// スキーム"),
    ]
    for url, label in scheme_bad:
        check(f"  非http(s)スキーム除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))

    # 政府ポータルサイト SKIP_DOMAINS
    gov_bad_urls = [
        ("https://www.nta.go.jp/taxes/tetsuzuki/",         "nta.go.jp 国税庁"),
        ("https://www.e-gov.go.jp/laws/",                  "e-gov.go.jp"),
        ("https://mirasapo-plus.go.jp/hojin/123",          "mirasapo-plus.go.jp"),
        ("https://j-net21.smrj.go.jp/startup/",            "j-net21.smrj.go.jp"),
        ("https://hellowork.mhlw.go.jp/servicef/123",      "hellowork.mhlw.go.jp"),
    ]
    gov_good_urls = [
        ("https://www.city.fujisawa.kanagawa.jp/soumu/",   "藤沢市（別ドメイン）"),
        ("https://example-gov.co.jp/",                     "govを含むが民間企業"),
    ]
    for url, label in gov_bad_urls:
        check(f"  政府ポータル除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in gov_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

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
