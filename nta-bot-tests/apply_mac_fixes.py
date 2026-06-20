#!/usr/bin/env python3
"""
Mac上の enricher.py と watchdog.py に累積的な改善を適用するパッチスクリプト。

使い方:
  python3 apply_mac_fixes.py                       # デフォルトパス
  python3 apply_mac_fixes.py ~/Downloads/nta-bot/tools/nta/bot/

パッチ一覧:
  E01  SKIP_DOMAINS: 求人・口コミ・レビュー・政府・URLショートナー追加
  E02  _BAD_PATH_PATTERNS: 企業情報DBシグナル検出
  E03  _BAD_SCHEMES / _BAD_PORTS / _MAX_URL_LEN: is_bad_url 強化
  E04  _VALID_SCHEMES / IP除外: _is_valid_result_url 強化
  E05  normalize_url: None安全・小文字化・デフォルトポート除去・UTM除去
  E06  write_scrape_results: no_url → url_failed 追加
  W01  reset_stuck: url_found(hp_url=NULL) → url_failed 追加
"""

import ast
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
RST = "\033[0m"

BOT_DIR = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 \
    else Path.home() / "Downloads/nta-bot/tools/nta/bot"
ENRICHER = BOT_DIR / "enricher.py"
WATCHDOG = BOT_DIR / "watchdog.py"

applied = []
skipped = []
failed = []


def ok(msg):
    print(f"  {GREEN}✓{RST}  {msg}")
    applied.append(msg)


def skip(msg):
    print(f"  {YELLOW}–{RST}  {msg} (既適用)")
    skipped.append(msg)


def fail(msg, detail=""):
    print(f"  {RED}✗{RST}  {msg}" + (f"\n      {detail}" if detail else ""))
    failed.append(msg)


def backup(path: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_suffix(f".py.bak_{ts}")
    shutil.copy2(path, bak)
    return bak


def verify_syntax(path: Path) -> bool:
    result = subprocess.run(
        [sys.executable, "-m", "py_compile", str(path)],
        capture_output=True, text=True,
    )
    return result.returncode == 0


def patch(path: Path, name: str, marker: str, old: str, new: str) -> bool:
    """
    ファイルを読み込み、old を new に置換する。
    marker が既にあれば適用済みとしてスキップ。
    old が見つからなければ失敗として報告。
    """
    text = path.read_text(encoding="utf-8")
    if marker in text:
        skip(name)
        return True
    if old not in text:
        fail(name, f"パターン未検出 — 手動確認が必要: {old[:60]!r}…")
        return False
    text = text.replace(old, new, 1)
    path.write_text(text, encoding="utf-8")
    if not verify_syntax(path):
        # ロールバック
        path.write_text(path.read_text(encoding="utf-8").replace(new, old, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")
        return False
    ok(name)
    return True


def patch_append(path: Path, name: str, marker: str, anchor: str, addition: str) -> bool:
    """anchor の直後に addition を挿入する"""
    text = path.read_text(encoding="utf-8")
    if marker in text:
        skip(name)
        return True
    if anchor not in text:
        fail(name, f"アンカー未検出: {anchor[:60]!r}…")
        return False
    text = text.replace(anchor, anchor + addition, 1)
    path.write_text(text, encoding="utf-8")
    if not verify_syntax(path):
        path.write_text(text.replace(anchor + addition, anchor, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")
        return False
    ok(name)
    return True


# ═══════════════════════════════════════════════════════════
#  enricher.py パッチ
# ═══════════════════════════════════════════════════════════

def patch_e01_skip_domains(text_orig: str, path: Path) -> str:
    """SKIP_DOMAINS: 求人・口コミ・レビュー・政府・URLショートナー追加"""
    name = "E01 SKIP_DOMAINS 拡張"
    marker = "# URLショートナー"

    text = path.read_text(encoding="utf-8")
    if marker in text:
        skip(name)
        return text

    # 既存 SKIP_DOMAINS の末尾を探して追記
    # 口コミ・評判サイト行の後ろに追加
    add_block = """
    # 求人サイト（企業の公式HPではなく求人ページ）
    "indeed.com", "doda.jp", "rikunabi.com", "mynavi.jp",
    "en-japan.com", "type.jp", "hatarako.net", "job-gear.jp",
    # 口コミ・評判サイト
    "glassdoor.com", "vorkers.com", "openwork.jp",
    # 飲食店・観光レビュー・予約サイト（企業HPではなくポータル）
    "tabelog.com", "retty.me", "hotpepper.jp", "jalan.net",
    "tripadvisor.jp", "tripadvisor.com", "booking.com", "yelp.co.jp",
    # 政府・行政ポータル（企業自身のサイトではない）
    "nta.go.jp", "e-gov.go.jp", "mirasapo-plus.go.jp",
    "j-net21.smrj.go.jp", "hellowork.mhlw.go.jp",
    # URLショートナー（最終的な企業HPではない）
    "bit.ly", "tinyurl.com", "t.co", "goo.gl",
    "ow.ly", "short.io", "lnkd.in", "ift.tt", "buff.ly","""

    # 既知のパターンを探す (SKIP_DOMAINS の閉じ括弧)
    patterns_to_try = [
        ('    "mens-aso.co.jp",\n])', '    "mens-aso.co.jp",' + add_block + '\n])'),
        ('    "mens-aso.co.jp"\n])', '    "mens-aso.co.jp",' + add_block + '\n])'),
        ('"athome.co.jp", "mens-aso.co.jp",\n])',
         '"athome.co.jp", "mens-aso.co.jp",' + add_block + '\n])'),
    ]
    for old, new in patterns_to_try:
        if old in text:
            text = text.replace(old, new, 1)
            path.write_text(text, encoding="utf-8")
            if verify_syntax(path):
                ok(name)
                return text
            path.write_text(text.replace(new, old, 1), encoding="utf-8")
            fail(name, "構文エラー — ロールバック済み")
            return path.read_text(encoding="utf-8")

    fail(name, "SKIP_DOMAINS の末尾パターンが見つかりません — 手動確認が必要")
    return text


def patch_e02_bad_path_patterns(path: Path):
    """E02 _BAD_PATH_PATTERNS: 企業情報DBシグナル検出"""
    name = "E02 _BAD_PATH_PATTERNS 追加"
    marker = "_BAD_PATH_PATTERNS"

    # 追加するコード (_is_valid_result_url や is_bad_url の前に入れる)
    new_code = """
import re as _re_mod

_BAD_PATH_PATTERNS = _re_mod.compile(
    r"/(hojin|houjin|kaisha|corporate_number|company-info|biz-info|hojinbango)"
    r"(?=[/?#]|$)"
    r"|[?&](corporate_number|hojin_id|company_id)=",
    _re_mod.IGNORECASE,
)

"""
    # _is_valid_result_url の直前に挿入
    anchors = ["def _is_valid_result_url(", "def is_valid_result_url("]
    text = path.read_text(encoding="utf-8")
    if marker in text:
        skip(name)
        return
    for anchor in anchors:
        if anchor in text:
            return patch_append(path, name, marker, anchor, "")
    # アンカーが見つからない → SKIP_DOMAINS の下に追加
    anchor2 = "\nSKIP_DOMAINS = frozenset"
    if anchor2 in text:
        idx = text.find(anchor2)
        # SKIP_DOMAINS ブロックの終端を探す
        end = text.find("\n])", idx)
        if end != -1:
            insert_at = end + 3  # "])" の後
            new_text = text[:insert_at] + "\n" + new_code + text[insert_at:]
            path.write_text(new_text, encoding="utf-8")
            if verify_syntax(path):
                ok(name)
                return
            path.write_text(text, encoding="utf-8")
            fail(name, "構文エラー — ロールバック済み")
            return
    fail(name, "挿入箇所が見つかりません")


def patch_e03_bad_schemes_ports(path: Path):
    """E03 _BAD_SCHEMES / _BAD_PORTS / _MAX_URL_LEN + is_bad_url 強化"""
    name = "E03 is_bad_url 強化 (_BAD_SCHEMES/_BAD_PORTS/_MAX_URL_LEN)"
    text = path.read_text(encoding="utf-8")
    if "_BAD_SCHEMES" in text:
        skip(name)
        return

    constants = '''
_BAD_SCHEMES = frozenset(["data", "javascript", "mailto", "tel", "ftp"])
_BAD_PORTS   = frozenset(["8080", "8443", "3000", "3001", "4000", "5000", "8000", "8888", "9000"])
_MAX_URL_LEN = 500
'''

    # is_bad_url 関数の先頭に None/length チェックを追加
    old_func_start = "def is_bad_url(url: str) -> bool:"
    new_func_start = "def is_bad_url(url) -> bool:"
    old_body_start = "    try:\n        p = urlparse(url)"
    new_body_start = (
        "    if not url or len(url) > _MAX_URL_LEN:\n"
        "        return True\n"
        "    try:\n"
        "        p = urlparse(url)\n"
        "        if p.scheme in _BAD_SCHEMES:\n"
        "            return True\n"
        "        if p.port and str(p.port) in _BAD_PORTS:\n"
        "            return True"
    )

    # まず定数ブロックを is_bad_url の直前に挿入
    anchor_func = old_func_start if old_func_start in text else new_func_start
    if anchor_func not in text:
        fail(name, "is_bad_url 関数が見つかりません")
        return
    text = text.replace(anchor_func, constants + anchor_func, 1)
    # 次に関数本体を強化
    if old_body_start in text:
        text = text.replace(old_body_start, new_body_start, 1)

    path.write_text(text, encoding="utf-8")
    if verify_syntax(path):
        ok(name)
    else:
        path.write_text(path.read_text(encoding="utf-8").replace(
            constants + anchor_func, anchor_func, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")


def patch_e04_valid_scheme_ip(path: Path):
    """E04 _is_valid_result_url: http/https のみ許可 + IPアドレス除外"""
    name = "E04 _is_valid_result_url スキーム/IP 検証"
    text = path.read_text(encoding="utf-8")
    if "_VALID_SCHEMES" in text:
        skip(name)
        return

    # _is_valid_result_url の先頭に追加
    # 関数の最初の try: の直後に追加
    old = """def _is_valid_result_url(url"""
    if old not in text:
        fail(name, "_is_valid_result_url 関数が見つかりません")
        return

    valid_schemes_const = '\n_VALID_SCHEMES = frozenset(["http", "https"])\n'

    # _BAD_PATH_PATTERNS の後 or SKIP_DOMAINS の後に定数を追加
    if "_BAD_PATH_PATTERNS" in text:
        anchor = "_BAD_PATH_PATTERNS = "
        end_of_pattern = text.find("\n)", text.find(anchor))
        if end_of_pattern == -1:
            end_of_pattern = text.find("\n    re.IGNORECASE,\n)", text.find(anchor))
        # パターン定義の直後の行末 +1
        insert_at = text.find("\n", end_of_pattern) + 1
        text = text[:insert_at] + valid_schemes_const + text[insert_at:]
    else:
        text = text.replace(old, valid_schemes_const + old, 1)

    # 関数本体に ip_address チェックを追加
    # 関数内部の最初の try: の後
    new_check = """        import ipaddress as _ipa
        if p.scheme not in _VALID_SCHEMES:
            return False
        _h = p.netloc.removeprefix("www.")
        if not _h:
            return False
        _host = _h[1:_h.index("]")] if _h.startswith("[") else _h.split(":")[0]
        try:
            _ipa.ip_address(_host)
            return False
        except ValueError:
            pass
"""
    # 既存の netloc チェック部分を探して置き換え
    old_check1 = "        h = p.netloc.removeprefix(\"www.\")\n        if not h:\n            return False"
    old_check2 = '        netloc = p.netloc.removeprefix("www.")\n        if not netloc:\n            return False'
    if old_check1 in text:
        text = text.replace(old_check1,
            '        _h = p.netloc.removeprefix("www.")\n'
            '        h = _h\n'
            '        if not h:\n            return False\n'
            '        import ipaddress as _ipa\n'
            '        _host = h[1:h.index("]")] if h.startswith("[") else h.split(":")[0]\n'
            '        try:\n            _ipa.ip_address(_host)\n            return False\n'
            '        except ValueError:\n            pass', 1)

    # scheme check の追加 (関数 try: の直後)
    old_try = "    try:\n        p = urlparse(url)\n        h = p.netloc"
    new_try = ("    try:\n"
               "        p = urlparse(url)\n"
               "        if p.scheme not in _VALID_SCHEMES:\n"
               "            return False\n"
               "        h = p.netloc")
    if old_try in text and "_VALID_SCHEMES" in text:
        # Already have _VALID_SCHEMES, just add the check
        text = text.replace(old_try, new_try, 1)

    path.write_text(text, encoding="utf-8")
    if verify_syntax(path):
        ok(name)
    else:
        fail(name, "構文エラー — 手動確認が必要 (ロールバックなし)")


def patch_e05_normalize_url(path: Path):
    """E05 normalize_url 関数追加/更新"""
    name = "E05 normalize_url 追加"
    text = path.read_text(encoding="utf-8")
    if "normalize_url" in text:
        skip(name)
        return

    func_code = '''

_UTM_PARAMS = frozenset([
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "msclkid", "yclid",
])
_DEFAULT_PORTS = {"http": "80", "https": "443"}


def normalize_url(url):
    """UTM除去・小文字化・デフォルトポート除去・フラグメント除去"""
    if not url:
        return None
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return None
        scheme = p.scheme.lower()
        host = p.hostname or ""
        port = p.port
        if port and str(port) == _DEFAULT_PORTS.get(scheme):
            netloc = host.lower()
        else:
            netloc = p.netloc.lower()
        if p.query:
            pairs = [kv for kv in p.query.split("&")
                     if kv.split("=")[0].lower() not in _UTM_PARAMS]
            query = "&".join(pairs)
        else:
            query = ""
        return urlunparse((scheme, netloc, p.path, p.params, query, "")) or None
    except Exception:
        return None

'''

    # SKIP_DOMAINS の前 or ファイル末尾に追加
    anchor = "\nSKIP_DOMAINS = frozenset"
    if anchor in text:
        text = text.replace(anchor, func_code + anchor, 1)
    else:
        text += func_code

    path.write_text(text, encoding="utf-8")
    if verify_syntax(path):
        ok(name)
    else:
        path.write_text(text.replace(func_code, "", 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")


def patch_e06_no_url_url_failed(path: Path):
    """E06 write_scrape_results: no_url → url_failed"""
    name = "E06 write_scrape_results no_url → url_failed"
    text = path.read_text(encoding="utf-8")

    # 既に no_url が含まれていればスキップ
    if '"no_url"' in text and "url_failed" in text:
        # より厳密にチェック
        if 'error in ("bad_url", "not_found", "blocked", "no_url")' in text \
                or "no_url" in text:
            skip(name)
            return

    # よくある write_scrape_results のパターン
    old1 = 'error in ("bad_url", "not_found", "blocked")'
    new1 = 'error in ("bad_url", "not_found", "blocked", "no_url")'
    if old1 in text:
        text = text.replace(old1, new1, 1)
        path.write_text(text, encoding="utf-8")
        if verify_syntax(path):
            ok(name)
            return
        path.write_text(text.replace(new1, old1, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")
        return

    old2 = "error in ['bad_url', 'not_found', 'blocked']"
    new2 = "error in ['bad_url', 'not_found', 'blocked', 'no_url']"
    if old2 in text:
        text = text.replace(old2, new2, 1)
        path.write_text(text, encoding="utf-8")
        if verify_syntax(path):
            ok(name)
            return
        path.write_text(text.replace(new2, old2, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")
        return

    fail(name, "write_scrape_results の bad_url パターンが見つかりません")


# ═══════════════════════════════════════════════════════════
#  watchdog.py パッチ
# ═══════════════════════════════════════════════════════════

def patch_w01_null_url_watchdog(path: Path):
    """W01 reset_stuck: url_found(hp_url=NULL) → url_failed"""
    name = "W01 watchdog url_found(hp_url=NULL) → url_failed"
    text = path.read_text(encoding="utf-8")
    if "hp_url IS NULL" in text and "url_failed" in text:
        skip(name)
        return

    # reset_stuck 内の commit() の前に追加
    old_commit = "    conn.commit()\n    logger.info"
    new_commit = (
        "    # url_found で hp_url=NULL → url_failed（URL再検索キューへ）\n"
        "    n_null = conn.execute(\n"
        "        \"UPDATE crawl_queue SET status='url_failed', attempts=0 \"\n"
        "        \"WHERE status='url_found' AND hp_url IS NULL\"\n"
        "    ).rowcount\n"
        "    if n_null:\n"
        "        logger.info(f'url_found(hp_url=NULL) → url_failed: {n_null}件')\n"
        "    conn.commit()\n"
        "    logger.info"
    )
    if old_commit in text:
        text = text.replace(old_commit, new_commit, 1)
        path.write_text(text, encoding="utf-8")
        if verify_syntax(path):
            ok(name)
            return
        path.write_text(text.replace(new_commit, old_commit, 1), encoding="utf-8")
        fail(name, "構文エラー — ロールバック済み")
        return

    # logger.info がない場合の代替パターン
    old_commit2 = "    db.commit()\n"
    if old_commit2 in text:
        new_commit2 = (
            "    n_null = db.execute(\n"
            "        \"UPDATE crawl_queue SET status='url_failed', attempts=0 \"\n"
            "        \"WHERE status='url_found' AND hp_url IS NULL\"\n"
            "    ).rowcount\n"
            "    db.commit()\n"
        )
        text = text.replace(old_commit2, new_commit2, 1)
        path.write_text(text, encoding="utf-8")
        if verify_syntax(path):
            ok(name)
            return
        path.write_text(text.replace(new_commit2, old_commit2, 1), encoding="utf-8")

    fail(name, "reset_stuck の commit パターンが見つかりません — 手動確認が必要")


# ═══════════════════════════════════════════════════════════
#  メイン
# ═══════════════════════════════════════════════════════════

def main():
    print(f"\n{'=' * 65}")
    print(f"  nta-bot パッチ適用スクリプト")
    print(f"  enricher: {ENRICHER}")
    print(f"  watchdog: {WATCHDOG}")
    print(f"{'=' * 65}\n")

    if not ENRICHER.exists():
        print(f"{RED}enricher.py が見つかりません: {ENRICHER}{RST}")
        sys.exit(1)
    if not WATCHDOG.exists():
        print(f"{RED}watchdog.py が見つかりません: {WATCHDOG}{RST}")
        sys.exit(1)

    # バックアップ
    bak_e = backup(ENRICHER)
    bak_w = backup(WATCHDOG)
    print(f"  バックアップ: {bak_e.name}")
    print(f"  バックアップ: {bak_w.name}\n")

    # ── enricher.py ──────────────────────────────────────────
    print("▼ enricher.py")
    e_text = ENRICHER.read_text(encoding="utf-8")
    patch_e01_skip_domains(e_text, ENRICHER)
    patch_e02_bad_path_patterns(ENRICHER)
    patch_e03_bad_schemes_ports(ENRICHER)
    patch_e04_valid_scheme_ip(ENRICHER)
    patch_e05_normalize_url(ENRICHER)
    patch_e06_no_url_url_failed(ENRICHER)

    # ── watchdog.py ──────────────────────────────────────────
    print("\n▼ watchdog.py")
    patch_w01_null_url_watchdog(WATCHDOG)

    # ── サマリー ─────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print(f"  適用: {len(applied)}件  スキップ(既適用): {len(skipped)}件  "
          f"失敗: {len(failed)}件")
    if failed:
        print(f"\n  {RED}✗ 手動確認が必要なパッチ:{RST}")
        for f in failed:
            print(f"    - {f}")
    print()

    if failed:
        print("  構文確認...")
        ok_e = verify_syntax(ENRICHER)
        ok_w = verify_syntax(WATCHDOG)
        print(f"    enricher.py: {'OK' if ok_e else 'NG'}")
        print(f"    watchdog.py: {'OK' if ok_w else 'NG'}")
        sys.exit(1)

    print(f"  {GREEN}全パッチ完了{RST}")
    print()
    print("  次のステップ:")
    print("    pkill -f watchdog.py")
    print("    nohup python3 ~/Downloads/nta-bot/tools/nta/bot/watchdog.py "
          "> ~/Downloads/watchdog.log 2>&1 &")
    print()


if __name__ == "__main__":
    main()
