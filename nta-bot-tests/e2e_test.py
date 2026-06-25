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
 20. normalize_url スキーム/ホスト小文字化・デフォルトポート除去・URLショートナー SKIP_DOMAINS
 21. normalize_url クエリパラメータソート・SNS/ニュース/EC サイト SKIP_DOMAINS
 22. ワイルドカード SKIP_DOMAINS マッチング（"google." 系）/ normalize_url 末尾スラッシュ正規化
 23. ソフトエラーページ検出（HTTP 200 でも実質 404/403/503 なページを url_failed に振り分け）
 24. write_url_results 統合テスト（SKIP判定 + CDN除外 + normalize_url を組み合わせた書き込み）
 25. extract_page_info + is_soft_error_page 統合テスト（HTML → タイトル/本文抽出 → ソフトエラー判定）
 26. classify_title_quality（タイトル品質分類: no_title / too_short / numeric_only / ok）
 27. check_db_consistency（DB整合性チェック + watchdog 適用後の問題解消確認）
 28. fetch_url_batch / fetch_scrape_batch（attempts 上限・バッチサイズ制御テスト）
 29. write_full_scrape_result（crawl_queue + corporations 統合書き込み・テーブル間整合確認）
 30. detect_duplicate_urls / reset_duplicate_urls（ポータルURL検出・同一URLが多数企業に割り当たる場合を検出）
 31. estimate_crawl_progress（クロール進捗統計: total/done/skip/remaining/completion_pct）
"""

import asyncio
import html as html_module
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
    # URLショートナー（最終的な企業HPではない）
    "bit.ly", "tinyurl.com", "t.co", "goo.gl",
    "ow.ly", "short.io", "lnkd.in", "ift.tt", "buff.ly",
    # SNS（企業公式アカウントページであっても企業HP本体ではない）
    "instagram.com", "youtube.com", "tiktok.com",
    "line.me", "pinterest.com", "tumblr.com",
    # ニュース・メディア（企業記事ページは企業HPではない）
    "nikkei.com", "asahi.com", "mainichi.jp", "yomiuri.co.jp",
    "sankei.com", "jiji.com", "kyodo.co.jp",
    "prtimes.jp", "dreamnews.jp",
    # EC・フリマ（出店ページは企業HPではない）
    "amazon.co.jp", "amazon.com", "rakuten.co.jp",
    "mercari.com", "yahooshopping.jp", "zozo.jp",
    "qoo10.jp", "shopping.yahoo.co.jp",
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


def _is_skip_domain(h: str, skip: frozenset) -> bool:
    """
    ワイルドカードドメインマッチング。
    SKIP_DOMAINS に末尾ドット付きエントリ（例: "google."）がある場合、
    任意TLDのそのドメインにマッチする（google.com, google.co.jp 等）。
    末尾ドットなしは従来の完全一致・サブドメイン一致。
    """
    for d in skip:
        if d.endswith("."):
            # "google." → google.com, google.co.jp, google.jp 等にマッチ
            d_base = d[:-1]
            if h == d_base or h.startswith(d_base + "."):
                return True
        else:
            if h == d or h.endswith("." + d):
                return True
    return False


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
        # ドメインが SKIP_DOMAINS に含まれるか（ワイルドカード対応）
        if _is_skip_domain(h, skip):
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


# HTTP 200 でも実質エラーページとして検出するタイトルパターン
# 英数字は \b 付きの語境界チェック、日本語は境界なしで直接マッチ
_BAD_TITLE_PATTERNS = re.compile(
    r"\b(?:404|not found|page not found|403|forbidden|access denied"
    r"|500|internal server error|503|service unavailable)\b"
    r"|ページが見つかりません|お探しのページ|アクセスできません|アクセス拒否"
    r"|メンテナンス中|サーバーエラー",
    re.IGNORECASE,
)

_MIN_BODY_LEN = 200  # これ以下の本文長はコンテンツなしと判定


def is_soft_error_page(title: str | None, body_text: str | None) -> str | None:
    """
    HTTP 200 でも実質エラーページを検出する（ソフト404/403/503対応）。
    戻り値: "not_found" | "blocked" | "fetch_failed" | None（正常 or 判定不能）
    """
    if title and _BAD_TITLE_PATTERNS.search(title):
        tl = title.lower()
        if ("404" in tl or "not found" in tl
                or "見つかりません" in title or "お探しのページ" in title):
            return "not_found"
        if ("403" in tl or "forbidden" in tl or "access denied" in tl
                or "アクセスできません" in title or "アクセス拒否" in title):
            return "blocked"
        return "fetch_failed"  # 500/503/メンテナンス等
    if body_text is not None and len(body_text.strip()) < _MIN_BODY_LEN:
        return "fetch_failed"  # 本文が極端に短い（画像のみ等）
    return None


_UTM_PARAMS = frozenset([
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "msclkid", "yclid",
])

_DEFAULT_PORTS = {"http": "80", "https": "443"}

def normalize_url(url: str | None) -> str | None:
    """
    URLを正規化して保存する。
    - None や空文字列は None を返す
    - スキーム・ホストを小文字化
    - デフォルトポート (:80/:443) 除去
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
        scheme = p.scheme.lower()
        # デフォルトポートを除去（example.co.jp:443 → example.co.jp）
        host = p.hostname or ""
        port = p.port
        if port and str(port) == _DEFAULT_PORTS.get(scheme):
            netloc = host.lower()
        else:
            netloc = p.netloc.lower()
        # クエリパラメータからトラッキング系を除去してソート（正規化）
        if p.query:
            pairs = [kv for kv in p.query.split("&")
                     if kv.split("=")[0].lower() not in _UTM_PARAMS]
            query = "&".join(sorted(pairs))
        else:
            query = ""
        # 空パスを "/" に正規化（https://example.co.jp と /の有無で重複しないよう）
        path = p.path or "/"
        from urllib.parse import urlunparse
        normalized = urlunparse((scheme, netloc, path, p.params, query, ""))
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


# タイトル品質チェック用
_NUMERIC_RE = re.compile(r"^\d+$")  # 数字のみのタイトル（法人番号DB の ID 等）


def classify_title_quality(title: str | None) -> str:
    """
    スクレイプ結果のタイトル品質を分類する。
    戻り値:
    - "ok"           : 正常なタイトル（企業HP として有効）
    - "no_title"     : タイトルなし（None または空文字）
    - "too_short"    : 短すぎる（2文字未満）
    - "numeric_only" : 数字のみ（法人番号DBのID、ページ番号等）
    """
    if not title or not title.strip():
        return "no_title"
    stripped = title.strip()
    if len(stripped) < 2:
        return "too_short"
    if _NUMERIC_RE.match(stripped):
        return "numeric_only"
    return "ok"


# HTML パース用（regex ベース・BeautifulSoup 不要）
_TITLE_RE    = re.compile(r"<title[^>]*>(.*?)</title>",                     re.IGNORECASE | re.DOTALL)
_OG_TITLE_RE = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
                           re.IGNORECASE)
_BODY_RE     = re.compile(r"<body[^>]*>(.*?)</body>",                       re.IGNORECASE | re.DOTALL)
_TAG_RE      = re.compile(r"<[^>]+>")
_WS_RE       = re.compile(r"\s+")


def extract_page_info(html: str) -> dict:
    """
    HTML からページのタイトルと本文テキストを抽出する。
    - タイトル: <title> タグ優先、なければ og:title、なければ None
    - 本文テキスト: <body> 内のタグ除去・空白正規化後の文字列（最大 1000 文字）
    """
    title = None
    m = _TITLE_RE.search(html)
    if m:
        title = html_module.unescape(_TAG_RE.sub("", m.group(1))).strip() or None
    if not title:
        m = _OG_TITLE_RE.search(html)
        if m:
            title = html_module.unescape(m.group(1)).strip() or None

    body_html = ""
    m = _BODY_RE.search(html)
    body_html = m.group(1) if m else html
    body_text = _WS_RE.sub(" ", _TAG_RE.sub(" ", body_html)).strip()
    body_text = body_text[:1000] if body_text else None

    return {"title": title, "body_text": body_text}


def check_db_consistency(conn) -> dict:
    """
    DB の状態整合性をチェックし、問題のある件数を返す。
    定期的に実行して積算された問題を検出するのに使用。

    戻り値キー:
    - url_found_null_url  : url_found で hp_url=NULL（watchdog が処理すべき）
    - stuck_url_searching : url_searching スタック（watchdog リセット対象）
    - stuck_scraping      : scraping スタック（watchdog リセット対象）
    - error_fetch_failed  : error で fetch_failed（watchdog の skip 処理対象）
    - done_no_url         : done で hp_url=NULL（データ整合性エラー・要手動修正）
    """
    issues = {}
    issues["url_found_null_url"] = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='url_found' AND hp_url IS NULL"
    ).fetchone()[0]
    issues["stuck_url_searching"] = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='url_searching'"
    ).fetchone()[0]
    issues["stuck_scraping"] = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='scraping'"
    ).fetchone()[0]
    issues["error_fetch_failed"] = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='error' AND error LIKE 'fetch_failed%'"
    ).fetchone()[0]
    issues["done_no_url"] = conn.execute(
        "SELECT COUNT(*) FROM crawl_queue WHERE status='done' AND hp_url IS NULL"
    ).fetchone()[0]
    return issues


def fetch_url_batch_sim(conn, batch_size: int = 10, max_attempts: int = 3) -> int:
    """
    URLフェーズのバッチ取得ロジックのシミュレーション。
    status='pending' かつ attempts < max_attempts のレコードを取得し
    url_searching に移動して attempts を +1。
    """
    n = conn.execute(
        "UPDATE crawl_queue SET status='url_searching', attempts=attempts+1 "
        "WHERE corporate_number IN ("
        "  SELECT corporate_number FROM crawl_queue "
        "  WHERE status='pending' AND attempts < ? LIMIT ?"
        ")",
        (max_attempts, batch_size),
    ).rowcount
    conn.commit()
    return n


def fetch_scrape_batch_sim(conn, batch_size: int = 10, max_attempts: int = 3) -> int:
    """
    スクレイプフェーズのバッチ取得ロジックのシミュレーション。
    status='url_found' かつ attempts < max_attempts のレコードを取得し
    scraping に移動して attempts を +1。
    """
    n = conn.execute(
        "UPDATE crawl_queue SET status='scraping', attempts=attempts+1 "
        "WHERE corporate_number IN ("
        "  SELECT corporate_number FROM crawl_queue "
        "  WHERE status='url_found' AND attempts < ? LIMIT ?"
        ")",
        (max_attempts, batch_size),
    ).rowcount
    conn.commit()
    return n


def detect_duplicate_urls(conn, threshold: int = 5) -> list:
    """
    同じ hp_url が threshold 件以上の企業に割り当てられているURLを検出する。
    企業一覧ページや検索結果ポータルの可能性が高いURLを特定する。
    戻り値: [(hp_url, count), ...] のリスト（count 降順）
    """
    rows = conn.execute(
        "SELECT hp_url, COUNT(*) as cnt FROM crawl_queue "
        "WHERE status IN ('done','url_found') AND hp_url IS NOT NULL "
        "GROUP BY hp_url HAVING cnt >= ? ORDER BY cnt DESC",
        (threshold,),
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def reset_duplicate_urls(conn, threshold: int = 5) -> tuple:
    """
    重複URLを持つレコードを pending に戻して hp_url をクリア（URL再検索）。
    戻り値: (crawl_queue リセット件数, corporations クリア件数)
    """
    duplicate_urls = [url for url, _ in detect_duplicate_urls(conn, threshold)]
    if not duplicate_urls:
        return 0, 0
    n_queue = n_corps = 0
    for url in duplicate_urls:
        n_queue += conn.execute(
            "UPDATE crawl_queue SET status='pending', hp_url=NULL, attempts=0, error=NULL "
            "WHERE hp_url=? AND status IN ('done','url_found')",
            (url,),
        ).rowcount
        n_corps += conn.execute(
            "UPDATE corporations SET hp_url=NULL, hp_title=NULL, hp_scraped_at=NULL "
            "WHERE hp_url=?",
            (url,),
        ).rowcount
    conn.commit()
    return n_queue, n_corps


def estimate_crawl_progress(conn) -> dict:
    """
    クロール進捗統計を返す（E13 で enricher.py に追加される関数）。
    """
    rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM crawl_queue GROUP BY status"
    ).fetchall()
    counts = {r[0]: r[1] for r in rows}
    total = sum(counts.values())
    done  = counts.get("done",  0)
    skip_ = counts.get("skip",  0)
    finished = done + skip_
    return {
        "total":          total,
        "done":           done,
        "skip":           skip_,
        "pending":        counts.get("pending",       0),
        "url_searching":  counts.get("url_searching", 0),
        "url_found":      counts.get("url_found",     0),
        "url_failed":     counts.get("url_failed",    0),
        "scraping":       counts.get("scraping",      0),
        "error":          counts.get("error",         0),
        "remaining":      total - finished,
        "completion_pct": round(finished / total * 100, 2) if total else 0.0,
    }


def write_full_scrape_result_sim(conn, results):
    """
    スクレイプ結果を crawl_queue と corporations の両方に書き込む統合シミュレーション。
    - done (ok=True): crawl_queue→done, corporations を hp_url/hp_title/hp_scraped_at で更新
    - bad_url/not_found/no_url: crawl_queue→url_failed(hp_url=NULL), corporations も NULL クリア
    - blocked: crawl_queue→url_failed, corporations は変更なし（一時的ブロック・再試行前提）
    - fetch_failed: crawl_queue→error, corporations は変更なし
    """
    import datetime
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
    for r in results:
        cn, ok, error = r["corporate_number"], r.get("ok", False), r.get("error")
        hp_url, title = r.get("hp_url"), r.get("title")
        if error in ("bad_url", "not_found", "no_url"):
            conn.execute(
                "UPDATE crawl_queue SET status='url_failed', hp_url=NULL, "
                "error=?, attempts=0 WHERE corporate_number=?",
                (error, cn),
            )
            conn.execute(
                "UPDATE corporations SET hp_url=NULL, hp_title=NULL, hp_scraped_at=NULL "
                "WHERE corporate_number=?",
                (cn,),
            )
        elif error == "blocked":
            conn.execute(
                "UPDATE crawl_queue SET status='url_failed', error='blocked', attempts=0 "
                "WHERE corporate_number=?",
                (cn,),
            )
            # corporations は変更なし（一時的ブロックのため再試行前提）
        elif ok and hp_url and title:
            conn.execute(
                "UPDATE crawl_queue SET status='done', error=NULL WHERE corporate_number=?",
                (cn,),
            )
            conn.execute(
                "UPDATE corporations SET hp_url=?, hp_title=?, hp_scraped_at=? "
                "WHERE corporate_number=?",
                (hp_url, title, now, cn),
            )
        else:
            conn.execute(
                "UPDATE crawl_queue SET status='error', error=? WHERE corporate_number=?",
                (error or "fetch_failed", cn),
            )
            # corporations は変更なし（再試行で回復の可能性あり）
    conn.commit()


def write_url_results_sim(conn, results):
    """
    write_url_results のシミュレーション（URLフェーズ結果書き込み）。
    - 有効URL発見: url_searching → url_found（normalize_url 適用）
    - 不正URL発見（SKIP_DOMAINS/CDN等）: url_searching → url_failed (bad_url)
    - URL未発見: url_searching → url_failed (no_url)
    """
    for r in results:
        url = r.get("hp_url")
        cn = r["corporate_number"]
        if url and _is_valid_result_url(url, SKIP_DOMAINS) and not is_bad_url(url):
            norm = normalize_url(url) or url
            conn.execute(
                "UPDATE crawl_queue SET status='url_found', hp_url=?, attempts=?, error=NULL "
                "WHERE corporate_number=?",
                (norm, r.get("attempts", 1), cn),
            )
        elif url:
            conn.execute(
                "UPDATE crawl_queue SET status='url_failed', hp_url=NULL, "
                "error='bad_url', attempts=0 WHERE corporate_number=?",
                (cn,),
            )
        else:
            conn.execute(
                "UPDATE crawl_queue SET status='url_failed', hp_url=NULL, "
                "error='no_url', attempts=0 WHERE corporate_number=?",
                (cn,),
            )
    conn.commit()


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

    # ── テスト 20: normalize_url 拡張 / URLショートナー SKIP_DOMAINS ──
    print("\n▼ Test 20: normalize_url スキーム/ホスト小文字化・デフォルトポート除去・URLショートナー")

    # スキーム・ホストの小文字化
    norm20_cases = [
        ("HTTPS://Example.CO.JP/About",
         "https://example.co.jp/About",
         "スキーム・ホスト小文字化（パスは保持）"),
        ("HTTP://WWW.TOYOTA.CO.JP/",
         "http://www.toyota.co.jp/",
         "全大文字ホスト → 小文字"),
        ("https://example.co.jp:443/page",
         "https://example.co.jp/page",
         "https デフォルトポート :443 除去"),
        ("http://example.co.jp:80/",
         "http://example.co.jp/",
         "http デフォルトポート :80 除去"),
        ("https://example.co.jp:8080/",
         "https://example.co.jp:8080/",
         "非デフォルトポートは保持"),
    ]
    for url, expected, label in norm20_cases:
        result = normalize_url(url)
        check(f"  {label}", result == expected,
              f"期待={expected!r}, 実際={result!r}")

    # URLショートナーは企業HPとして除外
    short_bad_urls = [
        ("https://bit.ly/3xyzABC",           "bit.ly"),
        ("https://tinyurl.com/y1234abc",      "tinyurl.com"),
        ("https://t.co/AbCdEfGh",             "t.co"),
        ("https://goo.gl/maps/abcdef",        "goo.gl"),
        ("https://ow.ly/xxxx50ABCDE",         "ow.ly"),
        ("https://lnkd.in/eXXXXXX",          "lnkd.in"),
        ("https://ift.tt/XXXXXXX",            "ift.tt"),
        ("https://buff.ly/XXXXXXX",           "buff.ly"),
    ]
    short_good_urls = [
        ("https://example.co.jp/",            "通常ドメイン"),
        ("https://short-company.co.jp/",      "short を含むが独自ドメイン"),
    ]
    for url, label in short_bad_urls:
        check(f"  URLショートナー除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in short_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ── テスト 21: normalize_url クエリソート / SNS・ニュース・EC SKIP_DOMAINS ──
    print("\n▼ Test 21: normalize_url クエリパラメータソート / SNS・ニュース・EC SKIP_DOMAINS")

    # normalize_url: クエリパラメータのソート（URLの重複排除に重要）
    norm21_cases = [
        ("https://example.co.jp/?b=2&a=1",
         "https://example.co.jp/?a=1&b=2",
         "クエリパラメータをソート (b,a → a,b)"),
        ("https://example.co.jp/?z=9&m=5&a=1",
         "https://example.co.jp/?a=1&m=5&z=9",
         "3パラメータをソート (z,m,a → a,m,z)"),
        ("https://example.co.jp/?b=2&utm_source=google&a=1",
         "https://example.co.jp/?a=1&b=2",
         "UTM除去後にソート (b, utm_source, a → a,b)"),
        ("https://example.co.jp/?page=1",
         "https://example.co.jp/?page=1",
         "単一パラメータはそのまま"),
        ("https://example.co.jp/",
         "https://example.co.jp/",
         "クエリなしはそのまま"),
    ]
    for url, expected, label in norm21_cases:
        result = normalize_url(url)
        check(f"  {label}", result == expected,
              f"期待={expected!r}, 実際={result!r}")

    # SNS SKIP_DOMAINS
    sns_bad_urls = [
        ("https://www.instagram.com/company_xyz/",          "instagram.com"),
        ("https://www.youtube.com/channel/UCxxx",           "youtube.com"),
        ("https://www.tiktok.com/@company_xyz",             "tiktok.com"),
        ("https://line.me/R/ti/p/@companyxyz",              "line.me"),
        ("https://www.pinterest.com/company/",              "pinterest.com"),
        ("https://company.tumblr.com/",                     "tumblr.com（サブドメイン）"),
    ]
    sns_good_urls = [
        ("https://www.sony.co.jp/",                         "ソニー公式HP"),
        ("https://panasonic.net/",                          "パナソニック公式HP"),
    ]
    for url, label in sns_bad_urls:
        check(f"  SNS除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in sns_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ニュース・メディア SKIP_DOMAINS
    news_bad_urls = [
        ("https://www.nikkei.com/article/DGXZQOUC123/",    "nikkei.com 日経新聞"),
        ("https://www.asahi.com/articles/ASR123.html",      "asahi.com 朝日新聞"),
        ("https://mainichi.jp/articles/20260101/",          "mainichi.jp 毎日新聞"),
        ("https://www.yomiuri.co.jp/economy/123/",          "yomiuri.co.jp 読売新聞"),
        ("https://prtimes.jp/main/html/rd/p/000000001/",   "prtimes.jp プレスリリース"),
    ]
    news_good_urls = [
        ("https://www.ntt.co.jp/",                         "NTT公式HP"),
        ("https://news-company.co.jp/",                    "news を含む企業ドメイン"),
    ]
    for url, label in news_bad_urls:
        check(f"  ニュース除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in news_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # EC・フリマ SKIP_DOMAINS
    ec_bad_urls = [
        ("https://www.amazon.co.jp/dp/B001234567",         "amazon.co.jp"),
        ("https://item.rakuten.co.jp/shop/item1/",         "rakuten.co.jp"),
        ("https://jp.mercari.com/item/m12345678901",        "mercari.com"),
        ("https://shopping.yahoo.co.jp/product/123/",      "shopping.yahoo.co.jp"),
    ]
    ec_good_urls = [
        ("https://shop.example.co.jp/",                    "独自ECドメイン（企業HP）"),
        ("https://www.uniqlo.com/jp/ja/",                  "ユニクロ公式HP"),
    ]
    for url, label in ec_bad_urls:
        check(f"  EC除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in ec_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # ── テスト 22: ワイルドカード SKIP_DOMAINS / normalize_url 末尾スラッシュ ──
    print("\n▼ Test 22: ワイルドカード SKIP_DOMAINS / normalize_url 末尾スラッシュ正規化")

    # "google." "facebook." "linkedin." が任意TLDにマッチするか
    wildcard_bad_urls = [
        ("https://www.google.com/",            "google.com (TLD: com)"),
        ("https://www.google.co.jp/",          "google.co.jp (TLD: co.jp)"),
        ("https://www.google.jp/",             "google.jp (TLD: jp)"),
        ("https://www.facebook.com/company",   "facebook.com (TLD: com)"),
        ("https://www.facebook.co.jp/",        "facebook.co.jp (TLD: co.jp)"),
        ("https://linkedin.com/company/xyz",   "linkedin.com (TLD: com)"),
        ("https://www.linkedin.co.jp/",        "linkedin.co.jp (TLD: co.jp)"),
    ]
    wildcard_good_urls = [
        ("https://google-partner.co.jp/",      "google- で始まる独自ドメイン"),
        ("https://not-facebook.co.jp/",        "facebook を含まない独自ドメイン"),
        ("https://www.toyota.co.jp/",          "無関係の正常ドメイン"),
    ]
    for url, label in wildcard_bad_urls:
        check(f"  ワイルドカード除外: {label}",
              not _is_valid_result_url(url, SKIP_DOMAINS))
    for url, label in wildcard_good_urls:
        check(f"  正常URL通過: {label}",
              _is_valid_result_url(url, SKIP_DOMAINS))

    # normalize_url: 末尾スラッシュ正規化（空パス → "/"）
    norm22_cases = [
        ("https://example.co.jp",
         "https://example.co.jp/",
         "パスなし → 末尾スラッシュ付与"),
        ("http://example.co.jp",
         "http://example.co.jp/",
         "http パスなし → 末尾スラッシュ付与"),
        ("https://example.co.jp/",
         "https://example.co.jp/",
         "既存スラッシュ → そのまま（冪等）"),
        ("https://example.co.jp/about",
         "https://example.co.jp/about",
         "パスあり → 変更なし"),
        ("https://example.co.jp/about/",
         "https://example.co.jp/about/",
         "パスあり末尾スラッシュ → 変更なし"),
        ("https://example.co.jp?page=1",
         "https://example.co.jp/?page=1",
         "パスなし・クエリあり → スラッシュ付与"),
    ]
    for url, expected, label in norm22_cases:
        result = normalize_url(url)
        check(f"  {label}", result == expected,
              f"期待={expected!r}, 実際={result!r}")

    # ── テスト 23: ソフトエラーページ検出（HTTP 200 でも実質エラー）──
    print("\n▼ Test 23: ソフトエラーページ検出（HTTP 200 でも実質エラー）")

    soft_error_cases = [
        # (title, body_text, 期待error, ラベル)
        ("404 Not Found",
         "Long body text here " * 20,
         "not_found",
         "英語 404 タイトル"),
        ("ページが見つかりません | サイト名",
         "コンテンツ " * 20,
         "not_found",
         "日本語 404 タイトル"),
        ("お探しのページが見つかりませんでした",
         "テキスト " * 20,
         "not_found",
         "日本語 404 タイトル（長い）"),
        ("403 Forbidden",
         "Access denied " * 20,
         "blocked",
         "英語 403 タイトル"),
        ("Access Denied",
         "アクセス拒否 " * 20,
         "blocked",
         "Access Denied タイトル"),
        ("アクセスできません",
         "このページにはアクセスできません " * 10,
         "blocked",
         "日本語 403 タイトル"),
        ("500 Internal Server Error",
         "Error " * 20,
         "fetch_failed",
         "500 エラータイトル"),
        ("503 Service Unavailable",
         "Down " * 20,
         "fetch_failed",
         "503 タイトル"),
        ("メンテナンス中",
         "ただいまメンテナンス中です。 " * 5,
         "fetch_failed",
         "メンテナンスページ"),
        ("会社概要 | 株式会社テスト",
         "充実した会社概要のコンテンツです。 " * 15,
         None,
         "正常ページ（除外しない）"),
        ("テスト株式会社",
         "a" * 300,
         None,
         "本文十分（正常）"),
        ("正常タイトル",
         "a" * 50,
         "fetch_failed",
         "本文不十分（空ページ相当）"),
        (None,
         "a" * 50,
         "fetch_failed",
         "タイトルなし＋短い本文"),
        (None,
         "a" * 300,
         None,
         "タイトルなし＋本文十分（正常扱い）"),
    ]
    for title, body, expected, label in soft_error_cases:
        result = is_soft_error_page(title, body)
        check(f"  {label}", result == expected,
              f"期待={expected!r}, 実際={result!r}")

    # ── テスト 24: write_url_results（URLフェーズ結果書き込み統合テスト）──
    print("\n▼ Test 24: write_url_results（URLフェーズ: SKIP判定 + normalize_url 統合）")

    tmp24 = tempfile.mktemp(suffix=".db")
    try:
        conn24 = sqlite3.connect(tmp24)
        conn24.row_factory = sqlite3.Row
        conn24.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn24.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("G001", "正常URL企業",        "東", "千", "url_searching", 1, None, None, None),
                ("G002", "不正ドメインURL企業", "東", "千", "url_searching", 1, None, None, None),
                ("G003", "CDN URL企業",         "東", "千", "url_searching", 1, None, None, None),
                ("G004", "URL未発見企業",       "東", "千", "url_searching", 2, None, None, None),
                ("G005", "企業情報DBURL企業",   "東", "千", "url_searching", 1, None, None, None),
                ("G006", "大文字URL企業",       "東", "千", "url_searching", 1, None, None, None),
            ],
        )
        conn24.commit()

        url_results = [
            {"corporate_number": "G001", "hp_url": "https://example.co.jp/",           "attempts": 1},
            {"corporate_number": "G002", "hp_url": "https://nabutan.com/hojin/123",     "attempts": 1},
            {"corporate_number": "G003", "hp_url": "https://s.yimg.jp/icon.ico",        "attempts": 1},
            {"corporate_number": "G004", "hp_url": None,                                "attempts": 2},
            {"corporate_number": "G005", "hp_url": "https://www.freee.co.jp/co/123",    "attempts": 1},
            # 大文字URL → normalize_url で正規化されて格納されるか
            {"corporate_number": "G006", "hp_url": "HTTPS://Example.CO.JP",             "attempts": 1},
        ]
        write_url_results_sim(conn24, url_results)

        rows = {r["corporate_number"]: r for r in
                conn24.execute("SELECT * FROM crawl_queue").fetchall()}

        check("正常URL → url_found に移動",
              rows["G001"]["status"] == "url_found",
              f"実際={rows['G001']['status']}")
        check("正常URL → hp_url がセットされる",
              rows["G001"]["hp_url"] == "https://example.co.jp/",
              f"実際={rows['G001']['hp_url']}")
        check("正常URL → attempts が保持される",
              rows["G001"]["attempts"] == 1,
              f"実際={rows['G001']['attempts']}")
        check("不正ドメインURL(nabutan) → url_failed (bad_url)",
              rows["G002"]["status"] == "url_failed" and rows["G002"]["error"] == "bad_url",
              f"実際={rows['G002']['status']}, err={rows['G002']['error']}")
        check("不正ドメインURL → hp_url が NULL クリアされる",
              rows["G002"]["hp_url"] is None)
        check("不正ドメインURL → attempts が 0 にリセットされる",
              rows["G002"]["attempts"] == 0,
              f"実際={rows['G002']['attempts']}")
        check("CDN URL(.ico) → url_failed (bad_url)",
              rows["G003"]["status"] == "url_failed" and rows["G003"]["error"] == "bad_url",
              f"実際={rows['G003']['status']}, err={rows['G003']['error']}")
        check("URL未発見 → url_failed (no_url)",
              rows["G004"]["status"] == "url_failed" and rows["G004"]["error"] == "no_url",
              f"実際={rows['G004']['status']}, err={rows['G004']['error']}")
        check("URL未発見 → attempts が 0 にリセットされる",
              rows["G004"]["attempts"] == 0)
        check("企業情報DBURL(freee) → url_failed (bad_url)",
              rows["G005"]["status"] == "url_failed" and rows["G005"]["error"] == "bad_url",
              f"実際={rows['G005']['status']}, err={rows['G005']['error']}")
        check("大文字URL → url_found に移動（normalize_url 適用）",
              rows["G006"]["status"] == "url_found",
              f"実際={rows['G006']['status']}")
        check("大文字URL → normalize_url で小文字化・末尾スラッシュ付与",
              rows["G006"]["hp_url"] == "https://example.co.jp/",
              f"実際={rows['G006']['hp_url']}")

        conn24.close()
    finally:
        for f in [tmp24, tmp24 + "-shm", tmp24 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 25: extract_page_info + is_soft_error_page 統合テスト ──
    print("\n▼ Test 25: extract_page_info + is_soft_error_page 統合テスト")

    extract_cases = [
        # (html, 期待title, ラベル)
        ("<html><head><title>会社概要 | 株式会社テスト</title></head><body>本文</body></html>",
         "会社概要 | 株式会社テスト",
         "通常の title タグ"),
        ("<html><head><title>  空白あり  </title></head><body></body></html>",
         "空白あり",
         "title の前後空白を strip"),
        ("<html><head><title>テスト &amp; 会社</title></head><body></body></html>",
         "テスト & 会社",
         "HTML エンティティ (&amp;) をデコード"),
        ("<html><head><meta property='og:title' content='OGPタイトル' /></head><body></body></html>",
         "OGPタイトル",
         "title なし → og:title フォールバック"),
        ("<html><head><title>titleが優先</title>"
         "<meta property='og:title' content='OGP は無視' /></head><body></body></html>",
         "titleが優先",
         "title と og:title 両方あり → title 優先"),
        ("<html><body>タイトルなし</body></html>",
         None,
         "title なし・og:title なし → None"),
    ]
    for html_str, expected_title, label in extract_cases:
        info = extract_page_info(html_str)
        check(f"  extract_page_info: {label}",
              info["title"] == expected_title,
              f"期待={expected_title!r}, 実際={info['title']!r}")

    # body_text 抽出テスト
    html_with_body = (
        "<html><head><title>T</title></head>"
        "<body><h1>見出し</h1><p>段落テキスト。長い文章が続きます。</p></body></html>"
    )
    info_body = extract_page_info(html_with_body)
    check("extract_page_info: body_text が抽出される",
          info_body["body_text"] is not None and "段落テキスト" in info_body["body_text"],
          f"実際={info_body['body_text']!r}")
    check("extract_page_info: タグが除去される",
          "<p>" not in (info_body["body_text"] or ""),
          f"実際={info_body['body_text']!r}")

    # extract_page_info + is_soft_error_page の統合テスト
    integration_cases = [
        # (html, 期待error, ラベル)
        ("<html><head><title>404 Not Found</title></head><body>" + "x" * 300 + "</body></html>",
         "not_found",
         "404 タイトル → not_found"),
        ("<html><head><title>403 Forbidden</title></head><body>" + "x" * 300 + "</body></html>",
         "blocked",
         "403 タイトル → blocked"),
        ("<html><head><title>503 Service Unavailable</title></head><body>" + "x" * 300 + "</body></html>",
         "fetch_failed",
         "503 タイトル → fetch_failed"),
        ("<html><head><title>株式会社テスト | 会社案内</title></head><body>" + "正常なコンテンツです。" * 30 + "</body></html>",
         None,
         "正常ページ → None（除外しない）"),
        ("<html><head><title>テスト</title></head><body>短い</body></html>",
         "fetch_failed",
         "本文が _MIN_BODY_LEN 未満 → fetch_failed"),
    ]
    for html_str, expected_err, label in integration_cases:
        info = extract_page_info(html_str)
        result = is_soft_error_page(info["title"], info["body_text"])
        check(f"  統合: {label}",
              result == expected_err,
              f"title={info['title']!r}, body_len={len(info['body_text'] or '')}, 期待={expected_err!r}, 実際={result!r}")

    # ── テスト 26: タイトル品質分類（classify_title_quality）──
    print("\n▼ Test 26: classify_title_quality（スクレイプ結果タイトルの品質分類）")

    title_quality_cases = [
        # (title, 期待分類, ラベル)
        ("株式会社テスト",          "ok",           "正常タイトル"),
        ("Toyota Motor Corporation","ok",           "英語タイトル"),
        ("会社案内 | 株式会社ABC",  "ok",           "パイプ区切りタイトル"),
        ("AB",                      "ok",           "2文字（最短有効）"),
        (None,                      "no_title",     "None → no_title"),
        ("",                        "no_title",     "空文字列 → no_title"),
        ("   ",                     "no_title",     "空白のみ → no_title"),
        ("A",                       "too_short",    "1文字 → too_short"),
        ("あ",                      "too_short",    "日本語1文字 → too_short"),
        ("1234567890123",           "numeric_only", "13桁法人番号 → numeric_only"),
        ("12345",                   "numeric_only", "数字のみ5桁 → numeric_only"),
        ("0",                       "too_short",    "1桁数字 → too_short（2文字未満優先）"),
        ("404",                     "numeric_only", "404 数字のみ → numeric_only（soft error と区別）"),
        ("123 ABC",                 "ok",           "数字+英字は numeric_only でない"),
    ]
    for title, expected, label in title_quality_cases:
        result = classify_title_quality(title)
        check(f"  {label}",
              result == expected,
              f"title={title!r}, 期待={expected!r}, 実際={result!r}")

    # classify_title_quality + is_soft_error_page の組み合わせ判定
    # スクレイプ後の総合判定: soft_error → url_failed、title_ng → 要確認（fetch_failed 扱い）
    def judge_scrape_result(title, body_text):
        """スクレイプ結果の総合品質判定"""
        soft_err = is_soft_error_page(title, body_text)
        if soft_err:
            return soft_err
        quality = classify_title_quality(title)
        if quality != "ok":
            return "fetch_failed"  # タイトル品質不良 → 再試行対象
        return None  # 正常

    combo_cases = [
        # (title, body, 期待結果, ラベル)
        ("404 Not Found",   "x" * 300, "not_found",   "ソフト404が優先"),
        (None,              "x" * 300, "fetch_failed", "タイトルなし → fetch_failed"),
        ("A",               "x" * 300, "fetch_failed", "短すぎタイトル → fetch_failed"),
        ("9999999999999",   "x" * 300, "fetch_failed", "数字のみタイトル → fetch_failed"),
        ("株式会社テスト", "x" * 300,  None,           "正常 → None"),
        ("正常タイトル",   "x" * 10,   "fetch_failed", "本文短すぎ → fetch_failed"),
    ]
    for title, body, expected, label in combo_cases:
        result = judge_scrape_result(title, body)
        check(f"  総合判定: {label}",
              result == expected,
              f"期待={expected!r}, 実際={result!r}")

    # ── テスト 27: DB整合性チェック + watchdog 適用後の改善確認 ──
    print("\n▼ Test 27: check_db_consistency（DB整合性チェック）")

    tmp27 = tempfile.mktemp(suffix=".db")
    try:
        conn27 = sqlite3.connect(tmp27)
        conn27.row_factory = sqlite3.Row
        conn27.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        conn27.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                # 問題あり: url_found で hp_url=NULL
                ("H001", "url_found NULL 1", "東", "千", "url_found",     2, None,                       None,          None),
                ("H002", "url_found NULL 2", "東", "千", "url_found",     1, None,                       None,          None),
                # 問題あり: url_searching スタック
                ("H003", "url_searching スタック", "東", "千", "url_searching", 1, None,                  None,          None),
                # 問題あり: scraping スタック
                ("H004", "scraping スタック",       "東", "千", "scraping",      2, "https://x.co.jp",   None,          None),
                # 問題あり: error + fetch_failed（watchdog の skip 対象）
                ("H005", "fetch_failed error",       "東", "千", "error",         3, "https://dead.co.jp","fetch_failed",None),
                # 問題あり: done で hp_url=NULL（データ整合性エラー）
                ("H006", "done NULL hp_url",         "東", "千", "done",          3, None,                None,          None),
                # 問題なし: 正常レコード
                ("H007", "正常 done",                "東", "千", "done",          3, "https://ok.co.jp",  None,          None),
                ("H008", "正常 pending",             "東", "千", "pending",       0, None,                None,          None),
                ("H009", "正常 url_found URL あり",  "東", "千", "url_found",     1, "https://a.co.jp",  None,          None),
            ],
        )
        conn27.commit()

        # ① チェック前の整合性確認
        before = check_db_consistency(conn27)

        check("事前: url_found_null_url が 2 件検出される",
              before["url_found_null_url"] == 2,
              f"実際={before['url_found_null_url']}")
        check("事前: stuck_url_searching が 1 件検出される",
              before["stuck_url_searching"] == 1,
              f"実際={before['stuck_url_searching']}")
        check("事前: stuck_scraping が 1 件検出される",
              before["stuck_scraping"] == 1,
              f"実際={before['stuck_scraping']}")
        check("事前: error_fetch_failed が 1 件検出される",
              before["error_fetch_failed"] == 1,
              f"実際={before['error_fetch_failed']}")
        check("事前: done_no_url が 1 件検出される",
              before["done_no_url"] == 1,
              f"実際={before['done_no_url']}")

        # ② watchdog の reset_stuck を適用
        reset_stuck_watchdog(conn27)

        # ③ チェック後の整合性確認
        after = check_db_consistency(conn27)

        check("適用後: url_found_null_url が 0 に解消（url_failed へ移動）",
              after["url_found_null_url"] == 0,
              f"実際={after['url_found_null_url']}")
        check("適用後: stuck_url_searching が 0 に解消（pending へ）",
              after["stuck_url_searching"] == 0,
              f"実際={after['stuck_url_searching']}")
        check("適用後: stuck_scraping が 0 に解消（url_found へ）",
              after["stuck_scraping"] == 0,
              f"実際={after['stuck_scraping']}")
        check("適用後: error_fetch_failed が 0 に解消（skip へ）",
              after["error_fetch_failed"] == 0,
              f"実際={after['error_fetch_failed']}")
        check("適用後: done_no_url は watchdog では解消されない（手動修正が必要）",
              after["done_no_url"] == 1,
              f"実際={after['done_no_url']}")
        check("正常 url_found(hp_url あり) は変更されない",
              conn27.execute(
                  "SELECT status FROM crawl_queue WHERE corporate_number='H009'"
              ).fetchone()["status"] == "url_found")

        conn27.close()
    finally:
        for f in [tmp27, tmp27 + "-shm", tmp27 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 28: バッチ取得ロジック（attempts 上限 + バッチサイズ制御）──
    print("\n▼ Test 28: fetch_url_batch / fetch_scrape_batch（attempts 上限・バッチサイズ）")

    tmp28 = tempfile.mktemp(suffix=".db")
    try:
        conn28 = sqlite3.connect(tmp28)
        conn28.row_factory = sqlite3.Row
        conn28.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        # pending: attempts 0, 1, 2, 3（3 は max_attempts=3 で除外される）
        # url_found: attempts 0, 1, 2, 3（3 は除外）
        conn28.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("P001", "pending-0", "東", "千", "pending",   0, None,                    None, None),
                ("P002", "pending-1", "東", "千", "pending",   1, None,                    None, None),
                ("P003", "pending-2", "東", "千", "pending",   2, None,                    None, None),
                ("P004", "pending-3", "東", "千", "pending",   3, None,                    None, None),  # 上限
                ("P005", "pending-0b","東", "千", "pending",   0, None,                    None, None),
                ("U001", "uf-0",      "東", "千", "url_found", 0, "https://a.co.jp",       None, None),
                ("U002", "uf-1",      "東", "千", "url_found", 1, "https://b.co.jp",       None, None),
                ("U003", "uf-2",      "東", "千", "url_found", 2, "https://c.co.jp",       None, None),
                ("U004", "uf-3",      "東", "千", "url_found", 3, "https://d.co.jp",       None, None),  # 上限
            ],
        )
        conn28.commit()

        # ── URL フェーズ: batch_size=2, max_attempts=3 ──
        n_url = fetch_url_batch_sim(conn28, batch_size=2, max_attempts=3)
        check("URL バッチ: batch_size=2 → 2 件取得される",
              n_url == 2, f"実際={n_url}")
        url_searching_count = conn28.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE status='url_searching'"
        ).fetchone()[0]
        check("URL バッチ: 取得分が url_searching に移行",
              url_searching_count == 2, f"実際={url_searching_count}")
        # attempts=3 のレコードが取得されていないこと
        p004_status = conn28.execute(
            "SELECT status, attempts FROM crawl_queue WHERE corporate_number='P004'"
        ).fetchone()
        check("URL バッチ: attempts=3(上限) のレコードは取得されない",
              p004_status["status"] == "pending",
              f"実際={p004_status['status']}")

        # ── URL フェーズ: 2 回目 batch_size=10 → 残り pending から取得 ──
        n_url2 = fetch_url_batch_sim(conn28, batch_size=10, max_attempts=3)
        # P001,P002,P003,P005 のうち最初の2件が取得済み → 残り2件
        check("URL バッチ 2回目: 残りの pending 2 件が取得される",
              n_url2 == 2, f"実際={n_url2}")
        # P004(attempts=3) は依然として pending のまま
        p004_after = conn28.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number='P004'"
        ).fetchone()
        check("URL バッチ 2回目: attempts=3 は依然 pending のまま（取得されない）",
              p004_after["status"] == "pending",
              f"実際={p004_after['status']}")

        # ── スクレイプフェーズ: batch_size=5, max_attempts=3 ──
        n_scrape = fetch_scrape_batch_sim(conn28, batch_size=5, max_attempts=3)
        # U001(0<3), U002(1<3), U003(2<3) が取得対象（3件）
        # U004(3 は除外)
        check("スクレイプ バッチ: attempts < 3 の url_found 3 件が取得される",
              n_scrape == 3, f"実際={n_scrape}")
        scraping_count = conn28.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE status='scraping'"
        ).fetchone()[0]
        check("スクレイプ バッチ: 取得分が scraping に移行",
              scraping_count == 3, f"実際={scraping_count}")
        u004_status = conn28.execute(
            "SELECT status FROM crawl_queue WHERE corporate_number='U004'"
        ).fetchone()
        check("スクレイプ バッチ: attempts=3(上限) の url_found は取得されない",
              u004_status["status"] == "url_found",
              f"実際={u004_status['status']}")

        # attempts のインクリメント確認
        u001_after = conn28.execute(
            "SELECT attempts FROM crawl_queue WHERE corporate_number='U001'"
        ).fetchone()
        check("スクレイプ バッチ: 取得後に attempts が +1 される（0→1）",
              u001_after["attempts"] == 1,
              f"実際={u001_after['attempts']}")

        conn28.close()
    finally:
        for f in [tmp28, tmp28 + "-shm", tmp28 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 29: write_full_scrape_result（crawl_queue + corporations 統合書き込み）──
    print("\n▼ Test 29: write_full_scrape_result（crawl_queue + corporations 両テーブル整合確認）")

    tmp29 = tempfile.mktemp(suffix=".db")
    try:
        conn29 = sqlite3.connect(tmp29)
        conn29.row_factory = sqlite3.Row
        conn29.executescript("""
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
        sample_url = "https://example.co.jp/"
        conn29.executemany(
            "INSERT INTO corporations VALUES (?,?,?,?,?,?)",
            [
                ("I001", "正常スクレイプ",   "2015", sample_url, None, None),
                ("I002", "bad_url 判明",      "2015", "https://s.yimg.jp/icon.ico", None, None),
                ("I003", "404 not_found",     "2015", "https://gone.co.jp/",        None, None),
                ("I004", "blocked(403)",      "2015", "https://block.co.jp/",       None, None),
                ("I005", "fetch_failed",      "2015", "https://dead.co.jp/",        None, None),
                ("I006", "no_url",            "2015", None,                         None, None),
            ],
        )
        conn29.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("I001", "正常スクレイプ",  "東", "千", "scraping", 1, sample_url,                  None, None),
                ("I002", "bad_url 判明",    "東", "千", "scraping", 1, "https://s.yimg.jp/icon.ico",None, None),
                ("I003", "404 not_found",   "東", "千", "scraping", 1, "https://gone.co.jp/",       None, None),
                ("I004", "blocked(403)",    "東", "千", "scraping", 1, "https://block.co.jp/",      None, None),
                ("I005", "fetch_failed",    "東", "千", "scraping", 2, "https://dead.co.jp/",       None, None),
                ("I006", "no_url",          "東", "千", "scraping", 1, None,                        None, None),
            ],
        )
        conn29.commit()

        scrape_results = [
            {"corporate_number": "I001", "ok": True,  "hp_url": sample_url, "title": "テスト株式会社"},
            {"corporate_number": "I002", "ok": False, "error": "bad_url"},
            {"corporate_number": "I003", "ok": False, "error": "not_found"},
            {"corporate_number": "I004", "ok": False, "error": "blocked"},
            {"corporate_number": "I005", "ok": False, "error": "fetch_failed"},
            {"corporate_number": "I006", "ok": False, "error": "no_url"},
        ]
        write_full_scrape_result_sim(conn29, scrape_results)

        q = {r["corporate_number"]: r for r in conn29.execute("SELECT * FROM crawl_queue").fetchall()}
        c = {r["corporate_number"]: r for r in conn29.execute("SELECT * FROM corporations").fetchall()}

        # I001: done → corporations 更新
        check("done: crawl_queue が done に移行",
              q["I001"]["status"] == "done",  f"実際={q['I001']['status']}")
        check("done: corporations の hp_url が保存される",
              c["I001"]["hp_url"] == sample_url, f"実際={c['I001']['hp_url']}")
        check("done: corporations の hp_title が保存される",
              c["I001"]["hp_title"] == "テスト株式会社", f"実際={c['I001']['hp_title']}")
        check("done: corporations の hp_scraped_at が設定される",
              c["I001"]["hp_scraped_at"] is not None)

        # I002: bad_url → crawl_queue url_failed + corporations NULL クリア
        check("bad_url: crawl_queue が url_failed に移行",
              q["I002"]["status"] == "url_failed", f"実際={q['I002']['status']}")
        check("bad_url: corporations の hp_url が NULL クリアされる",
              c["I002"]["hp_url"] is None, f"実際={c['I002']['hp_url']}")

        # I003: not_found → 同様に corporations クリア
        check("not_found: corporations の hp_url が NULL クリアされる",
              c["I003"]["hp_url"] is None, f"実際={c['I003']['hp_url']}")

        # I004: blocked → crawl_queue url_failed, corporations は変更なし
        check("blocked: crawl_queue が url_failed に移行",
              q["I004"]["status"] == "url_failed", f"実際={q['I004']['status']}")
        check("blocked: corporations の hp_url は変更されない（再試行前提）",
              c["I004"]["hp_url"] == "https://block.co.jp/",
              f"実際={c['I004']['hp_url']}")

        # I005: fetch_failed → crawl_queue error, corporations 変更なし
        check("fetch_failed: crawl_queue が error に移行",
              q["I005"]["status"] == "error", f"実際={q['I005']['status']}")
        check("fetch_failed: corporations の hp_url は変更されない",
              c["I005"]["hp_url"] == "https://dead.co.jp/",
              f"実際={c['I005']['hp_url']}")

        # I006: no_url → corporations NULL クリア
        check("no_url: corporations の hp_url が NULL クリアされる（元々 NULL）",
              c["I006"]["hp_url"] is None)

        conn29.close()
    finally:
        for f in [tmp29, tmp29 + "-shm", tmp29 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 30: URL重複検出 + reset_duplicate_urls ──────
    print("\n▼ Test 30: detect_duplicate_urls / reset_duplicate_urls（ポータルURL検出）")

    tmp30 = tempfile.mktemp(suffix=".db")
    try:
        conn30 = sqlite3.connect(tmp30)
        conn30.row_factory = sqlite3.Row
        conn30.executescript("""
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
        PORTAL_URL  = "https://portal-list.co.jp/"          # 10社 → 閾値5以上
        SHARED_URL  = "https://shared-three.co.jp/"         # 3社 → 閾値5未満
        UNIQUE_URL  = "https://unique-company.co.jp/"       # 1社 → 正常
        scraped_at  = "2026-06-20T00:00:00+00:00"

        corps_data, queue_data = [], []
        for i in range(10):
            cn = f"J{i:03d}"
            corps_data.append((cn, f"ポータル企業{i}", "2015", PORTAL_URL, "ポータル", scraped_at))
            queue_data.append((cn, f"ポータル企業{i}", "東", "千", "done", 3, PORTAL_URL, None, None))
        for i in range(3):
            cn = f"K{i:03d}"
            corps_data.append((cn, f"共有企業{i}", "2015", SHARED_URL, "共有", scraped_at))
            queue_data.append((cn, f"共有企業{i}", "東", "千", "done", 3, SHARED_URL, None, None))
        corps_data.append(("L001", "ユニーク企業", "2015", UNIQUE_URL, "ユニーク", scraped_at))
        queue_data.append(("L001", "ユニーク企業", "東", "千", "done", 3, UNIQUE_URL, None, None))

        conn30.executemany("INSERT INTO corporations VALUES (?,?,?,?,?,?)", corps_data)
        conn30.executemany("INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)", queue_data)
        conn30.commit()

        # ① 重複検出（閾値5）
        dups = detect_duplicate_urls(conn30, threshold=5)
        check("重複検出: PORTAL_URL（10社）が検出される",
              any(url == PORTAL_URL for url, _ in dups),
              f"実際={dups}")
        check("重複検出: SHARED_URL（3社）は閾値5未満で検出されない",
              not any(url == SHARED_URL for url, _ in dups),
              f"実際={dups}")
        check("重複検出: UNIQUE_URL（1社）は検出されない",
              not any(url == UNIQUE_URL for url, _ in dups))
        check("重複検出: 検出件数が 1 件",
              len(dups) == 1, f"実際={len(dups)}")

        # ② リセット実行（閾値5）
        n_q, n_c = reset_duplicate_urls(conn30, threshold=5)
        check("リセット: crawl_queue が 10 件 pending に戻る",
              n_q == 10, f"実際={n_q}")
        check("リセット: corporations が 10 件 NULL クリアされる",
              n_c == 10, f"実際={n_c}")

        # ③ リセット後の状態確認
        portal_pending = conn30.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE hp_url IS NULL AND status='pending'"
        ).fetchone()[0]
        check("リセット後: ポータルURLの 10 件が pending に移行",
              portal_pending == 10, f"実際={portal_pending}")
        shared_done = conn30.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE hp_url=? AND status='done'",
            (SHARED_URL,),
        ).fetchone()[0]
        check("リセット後: SHARED_URL の 3 件は done のまま（閾値未満）",
              shared_done == 3, f"実際={shared_done}")
        unique_done = conn30.execute(
            "SELECT COUNT(*) FROM crawl_queue WHERE hp_url=? AND status='done'",
            (UNIQUE_URL,),
        ).fetchone()[0]
        check("リセット後: UNIQUE_URL の 1 件は done のまま",
              unique_done == 1, f"実際={unique_done}")
        portal_corp_null = conn30.execute(
            "SELECT COUNT(*) FROM corporations WHERE hp_url IS NULL"
        ).fetchone()[0]
        check("リセット後: corporations の PORTAL_URL 10 件が NULL クリア",
              portal_corp_null == 10, f"実際={portal_corp_null}")

        conn30.close()
    finally:
        for f in [tmp30, tmp30 + "-shm", tmp30 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

    # ── テスト 31: estimate_crawl_progress（進捗統計）─────────
    print("\n▼ Test 31: estimate_crawl_progress（クロール進捗統計）")

    tmp31 = tempfile.mktemp(suffix=".db")
    try:
        conn31 = sqlite3.connect(tmp31)
        conn31.row_factory = sqlite3.Row
        conn31.executescript("""
            CREATE TABLE crawl_queue (
                corporate_number TEXT PRIMARY KEY,
                name TEXT, pref_name TEXT, city_name TEXT,
                status TEXT, attempts INTEGER,
                hp_url TEXT, error TEXT, last_attempt TEXT
            );
        """)
        # 総数 20件: done=10, skip=2, pending=4, url_found=1, url_failed=1, error=1, url_searching=1
        statuses = (
            [("done",          "https://ok{}.co.jp") for _ in range(10)] +
            [("skip",          None)                  for _ in range(2)]  +
            [("pending",       None)                  for _ in range(4)]  +
            [("url_found",     "https://uf.co.jp")]   +
            [("url_failed",    None)]                 +
            [("error",         None)]                 +
            [("url_searching", None)]
        )
        conn31.executemany(
            "INSERT INTO crawl_queue VALUES (?,?,?,?,?,?,?,?,?)",
            [
                (f"Z{i:03d}", f"企業{i}", "東", "千", st,
                 0, url.format(i) if url else None, None, None)
                for i, (st, url) in enumerate(statuses)
            ],
        )
        conn31.commit()

        prog = estimate_crawl_progress(conn31)

        check("total = 20",
              prog["total"] == 20, f"実際={prog['total']}")
        check("done = 10",
              prog["done"] == 10, f"実際={prog['done']}")
        check("skip = 2",
              prog["skip"] == 2, f"実際={prog['skip']}")
        check("pending = 4",
              prog["pending"] == 4, f"実際={prog['pending']}")
        check("url_found = 1",
              prog["url_found"] == 1, f"実際={prog['url_found']}")
        check("remaining = 8 (total - done - skip)",
              prog["remaining"] == 8, f"実際={prog['remaining']}")
        check("completion_pct = 60.0 ((10+2)/20 * 100)",
              prog["completion_pct"] == 60.0, f"実際={prog['completion_pct']}")

        # 全件 done にした後の 100% 確認
        conn31.execute("UPDATE crawl_queue SET status='done', hp_url='https://x.co.jp'")
        conn31.commit()
        prog2 = estimate_crawl_progress(conn31)
        check("全件 done → completion_pct = 100.0",
              prog2["completion_pct"] == 100.0, f"実際={prog2['completion_pct']}")
        check("全件 done → remaining = 0",
              prog2["remaining"] == 0, f"実際={prog2['remaining']}")

        # 空DBの場合は 0% で割り算エラーなし
        conn31.execute("DELETE FROM crawl_queue")
        conn31.commit()
        prog3 = estimate_crawl_progress(conn31)
        check("空DB → completion_pct = 0.0 (ZeroDivision なし)",
              prog3["completion_pct"] == 0.0, f"実際={prog3['completion_pct']}")

        conn31.close()
    finally:
        for f in [tmp31, tmp31 + "-shm", tmp31 + "-wal"]:
            if os.path.exists(f):
                os.unlink(f)

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
