#!/bin/bash
# enricher.py の lstrip("www.") バグ修正
# lstrip は文字の集合として引数を扱うため、"weblio.jp" → "eblio.jp" になってしまう
# removeprefix("www.") を使う必要がある
#
# 修正対象: ~/Downloads/nta-bot/tools/nta/bot/enricher.py
# 修正前:   h = urlparse(url).netloc.lstrip("www.")
# 修正後:   h = urlparse(url).netloc.removeprefix("www.")

ENRICHER=~/Downloads/nta-bot/tools/nta/bot/enricher.py

if [ ! -f "$ENRICHER" ]; then
    echo "ERROR: $ENRICHER が見つかりません"
    exit 1
fi

# バックアップ
cp "$ENRICHER" "${ENRICHER}.bak"
echo "バックアップ: ${ENRICHER}.bak"

# 修正
sed -i '' 's/\.netloc\.lstrip("www\.")/.netloc.removeprefix("www.")/g' "$ENRICHER"

# 確認
echo "修正後の該当行:"
grep -n 'removeprefix\|lstrip' "$ENRICHER"
echo "完了"
