#!/usr/bin/env bash
# nta-bot 進捗モニター
# 使い方: bash monitor.sh [DB_PATH] [LOG_PATH]
# 例:     bash monitor.sh ~/Downloads/houjin.db ~/Downloads/watchdog.log

DB="${1:-$HOME/Downloads/houjin.db}"
LOG="${2:-$HOME/Downloads/watchdog.log}"

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
BLU='\033[0;34m'; CYN='\033[0;36m'; BLD='\033[1m'; RST='\033[0m'

hr() { printf '%0.s─' {1..60}; echo; }

echo
echo -e "${BLD}━━━ nta-bot 進捗モニター $(date '+%Y-%m-%d %H:%M:%S') ━━━${RST}"
hr

# ── DB 統計 ──────────────────────────────────────────────────
if [ ! -f "$DB" ]; then
    echo -e "${RED}DB not found: $DB${RST}"
else
    echo -e "${BLD}▼ DB: $DB${RST}"
    sqlite3 "$DB" "
        SELECT
            printf('%-16s', status) || printf('%10s', format('%,d', count(*))) || ' 件'
        FROM crawl_queue
        GROUP BY status
        ORDER BY count(*) DESC
    " | while read line; do
        case "$line" in
            *done*)    echo -e "  ${GRN}${line}${RST}" ;;
            *pending*) echo -e "  ${BLU}${line}${RST}" ;;
            *skip*)    echo -e "  ${YLW}${line}${RST}" ;;
            *error*|*failed*) echo -e "  ${RED}${line}${RST}" ;;
            *)         echo "  $line" ;;
        esac
    done

    # 合計と完了率
    TOTAL=$(sqlite3 "$DB" "SELECT count(*) FROM crawl_queue")
    DONE=$(sqlite3 "$DB" "SELECT count(*) FROM crawl_queue WHERE status='done'")
    SKIP=$(sqlite3 "$DB" "SELECT count(*) FROM crawl_queue WHERE status='skip'")
    COMPLETE=$((DONE + SKIP))
    if [ "$TOTAL" -gt 0 ]; then
        PCT=$(awk "BEGIN {printf \"%.1f\", $COMPLETE/$TOTAL*100}")
        echo
        echo -e "  ${BLD}合計: $(printf '%,d' $TOTAL)件 / 完了(done+skip): $(printf '%,d' $COMPLETE)件 (${PCT}%)${RST}"
    fi

    # url_found のうち hp_url が NULL のもの（スタック確認）
    NULL_FOUND=$(sqlite3 "$DB" "SELECT count(*) FROM crawl_queue WHERE status='url_found' AND hp_url IS NULL")
    if [ "$NULL_FOUND" -gt 0 ]; then
        echo -e "  ${RED}⚠ url_found で hp_url=NULL: ${NULL_FOUND}件（スタック注意）${RST}"
    fi
fi

hr

# ── プロセス確認 ─────────────────────────────────────────────
echo -e "${BLD}▼ プロセス状態${RST}"
PS_WATCH=$(pgrep -f "watchdog.py" | head -5)
PS_ENRICH=$(pgrep -f "enricher.py" | head -5)

if [ -n "$PS_WATCH" ]; then
    echo -e "  ${GRN}✓ watchdog.py  PID=$(echo $PS_WATCH | tr '\n' ',')${RST}"
else
    echo -e "  ${RED}✗ watchdog.py が見つかりません（停止中？）${RST}"
fi

if [ -n "$PS_ENRICH" ]; then
    echo -e "  ${GRN}✓ enricher.py  PID=$(echo $PS_ENRICH | tr '\n' ',')${RST}"
else
    echo -e "  ${YLW}  enricher.py は現在停止（watchdog が管理中）${RST}"
fi

hr

# ── ログ末尾 ─────────────────────────────────────────────────
if [ -f "$LOG" ]; then
    echo -e "${BLD}▼ watchdog.log 末尾 15行 ($LOG)${RST}"
    tail -15 "$LOG" | while IFS= read -r line; do
        if echo "$line" | grep -qE "ERROR|失敗|✗|SKIP|skip"; then
            echo -e "  ${RED}${line}${RST}"
        elif echo "$line" | grep -qE "done|完了|✓|url_found"; then
            echo -e "  ${GRN}${line}${RST}"
        elif echo "$line" | grep -qE "pending|URL|起動"; then
            echo -e "  ${BLU}${line}${RST}"
        else
            echo "  $line"
        fi
    done
else
    echo -e "${YLW}ログファイルが見つかりません: $LOG${RST}"
fi

hr

# ── url_failed ドメイン簡易集計（上位10） ───────────────────
if [ -f "$DB" ]; then
    echo -e "${BLD}▼ url_failed ドメイン上位10件（ホスト別）${RST}"
    sqlite3 "$DB" "
        SELECT
            substr(hp_url, instr(hp_url,'://')+3,
                CASE
                    WHEN instr(substr(hp_url, instr(hp_url,'://')+3),'/') > 0
                    THEN instr(substr(hp_url, instr(hp_url,'://')+3),'/')-1
                    ELSE length(substr(hp_url, instr(hp_url,'://')+3))
                END
            ) as host,
            count(*) as cnt
        FROM crawl_queue
        WHERE status='url_failed' AND hp_url IS NOT NULL
        GROUP BY host
        ORDER BY cnt DESC
        LIMIT 10
    " | awk -F'|' '{printf "  %8s件  %s\n", $2, $1}'
fi

echo
