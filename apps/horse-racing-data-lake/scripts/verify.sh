#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
set -a
# shellcheck source=/dev/null
source "$APP_DIR/../../.env"
# shellcheck source=/dev/null
source "$APP_DIR/.env"
set +a

# テーブル固有トークン or 共通トークン
get_token() {
  local prefix="$1"
  local token_var="${prefix}_API_TOKEN"
  echo "${!token_var:-$CLOUDFLARE_API_TOKEN}"
}

echo "=== 1. horse_info テーブル ==="
HORSE_INFO_TOKEN=$(get_token "HORSE_INFO")
curl -s -w "\nHTTP %{http_code}\n" -X POST "$HORSE_INFO_STREAM_ENDPOINT" \
  -H "Authorization: Bearer $HORSE_INFO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "hi-001", "horse_hash": "h001", "horse_name": "Deep Impact",
    "horse_sex": "male", "foling_date": "2002-03-25T00:00:00Z",
    "foled_in": "Japan", "trainer_name": "Yasuo Ikee",
    "owner_name": "Kaneko Makoto", "breeder_name": "Northern Farm",
    "bloodline_sire": "Sunday Silence", "bloodline_dam": "Wind In Her Hair",
    "bloodline_grandsire": "Halo", "bloodline_granddam": "Wishing Well",
    "bloodline_maternal_grandsire": "Alzao", "bloodline_maternal_granddam": "Burghclere",
    "race_hash": "r001", "race_name": "Japan Derby",
    "race_date": "2005-05-29T00:00:00Z", "race_number": 11,
    "race_category": "G1", "race_course": "Tokyo",
    "race_organization": "JRA", "race_is_only_female": false,
    "race_surface": "turf", "race_distance": 2400,
    "race_direction": "left", "race_weather": "sunny", "race_condition": "good"
  }]'

echo ""
echo "=== 2. race_info テーブル ==="
RACE_INFO_TOKEN=$(get_token "RACE_INFO")
curl -s -w "\nHTTP %{http_code}\n" -X POST "$RACE_INFO_STREAM_ENDPOINT" \
  -H "Authorization: Bearer $RACE_INFO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "ri-001", "race_hash": "r001", "race_name": "Japan Derby",
    "race_date": "2005-05-29T00:00:00Z", "race_number": 11,
    "race_category": "G1", "race_course": "Tokyo",
    "race_organization": "JRA", "race_is_only_female": false,
    "race_surface": "turf", "race_distance": 2400,
    "race_direction": "left", "race_weather": "sunny", "race_condition": "good",
    "race_finishing_order": [1,2,3], "race_result_all_corner_positions": ["1-1-1"],
    "race_furlong_splits": [12.5, 11.8, 11.2],
    "ticket_tansho": [{"number": 1, "payout": 120, "popularity_rank": 1}],
    "ticket_fukusho": [{"number": 1, "payout": 110, "popularity_rank": 1}],
    "ticket_wakuren": [{"numbers": [1,2], "payout": 300, "popularity_rank": 1}],
    "ticket_umaren": [{"numbers": [1,2], "payout": 400, "popularity_rank": 1}],
    "ticket_wide": [{"numbers": [1,2], "payout": 200, "popularity_rank": 1}],
    "ticket_umatan": [{"numbers": [1,2], "payout": 500, "popularity_rank": 1}],
    "ticket_3renpuku": [{"numbers": [1,2,3], "payout": 600, "popularity_rank": 1}],
    "ticket_3rentan": [{"numbers": [1,2,3], "payout": 1200, "popularity_rank": 1}]
  }]'

echo ""
echo "=== 3. horse_racing_records テーブル ==="
HORSE_RACING_RECORDS_TOKEN=$(get_token "HORSE_RACING_RECORDS")
curl -s -w "\nHTTP %{http_code}\n" -X POST "$HORSE_RACING_RECORDS_STREAM_ENDPOINT" \
  -H "Authorization: Bearer $HORSE_RACING_RECORDS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "hrr-001", "horse_hash": "h001", "horse_name": "Deep Impact",
    "horse_sex": "male", "foling_date": "2002-03-25T00:00:00Z",
    "foled_in": "Japan", "trainer_name": "Yasuo Ikee",
    "owner_name": "Kaneko Makoto", "breeder_name": "Northern Farm",
    "bloodline_sire": "Sunday Silence", "bloodline_dam": "Wind In Her Hair",
    "bloodline_grandsire": "Halo", "bloodline_granddam": "Wishing Well",
    "bloodline_maternal_grandsire": "Alzao", "bloodline_maternal_granddam": "Burghclere",
    "race_hash": "r001", "race_name": "Japan Derby",
    "race_date": "2005-05-29T00:00:00Z", "race_number": 11,
    "race_category": "G1", "race_course": "Tokyo",
    "race_organization": "JRA", "race_is_only_female": false,
    "race_surface": "turf", "race_distance": 2400,
    "race_direction": "left", "race_weather": "sunny", "race_condition": "good",
    "race_finishing_order": [1,2,3], "race_result_all_corner_positions": ["1-1-1"],
    "race_furlong_splits": [12.5, 11.8, 11.2],
    "ticket_tansho": [{"number": 1, "payout": 120, "popularity_rank": 1}],
    "ticket_fukusho": [{"number": 1, "payout": 110, "popularity_rank": 1}],
    "ticket_wakuren": [{"numbers": [1,2], "payout": 300, "popularity_rank": 1}],
    "ticket_umaren": [{"numbers": [1,2], "payout": 400, "popularity_rank": 1}],
    "ticket_wide": [{"numbers": [1,2], "payout": 200, "popularity_rank": 1}],
    "ticket_umatan": [{"numbers": [1,2], "payout": 500, "popularity_rank": 1}],
    "ticket_3renpuku": [{"numbers": [1,2,3], "payout": 600, "popularity_rank": 1}],
    "ticket_3rentan": [{"numbers": [1,2,3], "payout": 1200, "popularity_rank": 1}],
    "finishing_position": 1, "gate_number": 3, "horse_number": 7,
    "sex_and_age": "male3", "weight_carried": 57,
    "jockey_name": "Yutaka Take", "time": 145.2, "margin": "",
    "horse_corner_positions": "1-1-1-1",
    "horse_closing_section_time": 34.5,
    "odds": 1.2, "odds_rank": 1, "horse_weight": 460, "prize_money": 200000000
  }]'

echo ""
echo "=== 4. 認証なしリクエスト → 拒否確認 ==="
curl -s -w "\nHTTP %{http_code}\n" -X POST "$HORSE_INFO_STREAM_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '[{"id": "test"}]'
echo "(400/401 が期待値)"

echo ""
echo "=== 5. R2 データ確認（60秒後に実行推奨） ==="
bunx wrangler r2 bucket catalog query "$HORSE_RACING_BUCKET_NAME" --query "SHOW TABLES IN horse_racing"
