#!/usr/bin/env bash
set -euo pipefail

ticker="${1:-VOO}"
upper=$(printf "%s" "$ticker" | tr '[:lower:]' '[:upper:]')
cleaned=$(printf "%s" "$upper" | sed 's/[^A-Z0-9.\-]//g')

if [[ -z "$cleaned" ]]; then
  cleaned="VOO"
fi

symbol="$cleaned"
if [[ "$symbol" != *.* ]]; then
  symbol="${symbol}.US"
fi
symbol_lower="${symbol,,}"

cookie_jar=$(mktemp)
cleanup() {
  rm -f "$cookie_jar"
}
trap cleanup EXIT

browser_ua="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"

common_headers=(
  --header "Accept-Language: en-US,en;q=0.9"
  --header "Cache-Control: no-cache"
  --header "Pragma: no-cache"
  --header "sec-ch-ua: \"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\""
  --header "sec-ch-ua-mobile: ?0"
  --header "sec-ch-ua-platform: \"Linux\""
)

curl_common_args=(
  --fail
  --show-error
  --silent
  --location
  --http1.1
  --compressed
  --max-time 30
  --connect-timeout 10
  --cookie-jar "$cookie_jar"
  --cookie "$cookie_jar"
  --user-agent "$browser_ua"
)

curl "${curl_common_args[@]}" \
  "${common_headers[@]}" \
  --header "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7" \
  --header "Sec-Fetch-Dest: document" \
  --header "Sec-Fetch-Mode: navigate" \
  --header "Sec-Fetch-Site: none" \
  --header "Sec-Fetch-User: ?1" \
  --header "Upgrade-Insecure-Requests: 1" \
  "https://stooq.com/" >/dev/null

curl "${curl_common_args[@]}" \
  "${common_headers[@]}" \
  --header "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7" \
  --header "Referer: https://stooq.com/" \
  --header "Sec-Fetch-Dest: document" \
  --header "Sec-Fetch-Mode: navigate" \
  --header "Sec-Fetch-Site: same-origin" \
  --header "Sec-Fetch-User: ?1" \
  --header "Upgrade-Insecure-Requests: 1" \
  "https://stooq.com/q/?s=${symbol_lower}" >/dev/null

curl "${curl_common_args[@]}" \
  "${common_headers[@]}" \
  --header "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7" \
  --header "Referer: https://stooq.com/q/?s=${symbol_lower}" \
  --header "Sec-Fetch-Dest: document" \
  --header "Sec-Fetch-Mode: navigate" \
  --header "Sec-Fetch-Site: same-origin" \
  --header "Sec-Fetch-User: ?1" \
  --header "Upgrade-Insecure-Requests: 1" \
  "https://stooq.com/q/d/?s=${symbol_lower}" >/dev/null

curl "${curl_common_args[@]}" \
  "${common_headers[@]}" \
  --header "Accept: text/csv,text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8" \
  --header "Referer: https://stooq.com/q/d/?s=${symbol_lower}" \
  --header "Sec-Fetch-Dest: document" \
  --header "Sec-Fetch-Mode: navigate" \
  --header "Sec-Fetch-Site: same-origin" \
  --header "Sec-Fetch-User: ?1" \
  --header "Upgrade-Insecure-Requests: 1" \
  "https://stooq.com/q/d/l/?s=${symbol_lower}&i=d"
