Table: horse_racing_records

```json
{
  "fields": [
    { "name": "id", "type": "string", "required": true },

    { "name": "horse_hash", "type": "string", "required": true },
    { "name": "horse_name", "type": "string", "required": true },
    { "name": "horse_sex", "type": "string", "required": true },
    { "name": "foling_date", "type": "timestamp", "required": true },
    { "name": "foled_in", "type": "string", "required": true },
    { "name": "trainer_name", "type": "string", "required": true },
    { "name": "owner_name", "type": "string", "required": true },
    { "name": "breeder_name", "type": "string", "required": true },

    { "name": "bloodline_sire", "type": "string", "required": true },
    { "name": "bloodline_dam", "type": "string", "required": true },
    { "name": "bloodline_grandsire", "type": "string", "required": true },
    { "name": "bloodline_granddam", "type": "string", "required": true },
    { "name": "bloodline_maternal_grandsire", "type": "string", "required": true },
    { "name": "bloodline_maternal_granddam", "type": "string", "required": true },

    { "name": "race_hash", "type": "string", "required": true },
    { "name": "race_name", "type": "string", "required": true },
    { "name": "race_date", "type": "timestamp", "required": true },
    { "name": "race_number", "type": "int32", "required": true },
    { "name": "race_category", "type": "string", "required": true },
    { "name": "race_course", "type": "string", "required": true },
    { "name": "race_organization", "type": "string", "required": true },
    { "name": "race_is_only_female", "type": "bool", "required": true },
    { "name": "race_surface", "type": "string", "required": true },
    { "name": "race_distance", "type": "int32", "required": true },
    { "name": "race_direction", "type": "string", "required": true },
    { "name": "race_weather", "type": "string", "required": true },
    { "name": "race_condition", "type": "string", "required": true },

    {
      "name": "race_finishing_order",
      "type": "list",
      "required": true,
      "items": { "type": "int32" }
    },
    {
      "name": "race_result_all_corner_positions",
      "type": "list",
      "required": true,
      "items": { "type": "string" }
    },
    {
      "name": "race_furlong_splits",
      "type": "list",
      "required": true,
      "items": { "type": "float64" }
    },

    {
      "name": "ticket_tansho",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          { "name": "number", "type": "int32", "required": true },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_fukusho",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          { "name": "number", "type": "int32", "required": true },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_wakuren",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_umaren",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_wide",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_umatan",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_3renpuku",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_3rentan",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },

    { "name": "finishing_position", "type": "int32", "required": true },
    { "name": "gate_number", "type": "int32", "required": true },
    { "name": "horse_number", "type": "int32", "required": true },
    { "name": "sex_and_age", "type": "string", "required": true },
    { "name": "weight_carried", "type": "int32", "required": true },
    { "name": "jockey_name", "type": "string", "required": true },
    { "name": "time", "type": "float64", "required": true },
    { "name": "margin", "type": "string", "required": true },
    { "name": "horse_corner_positions", "type": "string", "required": true },
    { "name": "horse_closing_section_time", "type": "float64", "required": true },
    { "name": "odds", "type": "float64", "required": true },
    { "name": "odds_rank", "type": "int32", "required": true },
    { "name": "horse_weight", "type": "int32", "required": true },
    { "name": "prize_money", "type": "float64", "required": true }
  ]
}
```

Table: horse_info

```json
{
  "fields": [
    { "name": "id", "type": "string", "required": true },

    { "name": "horse_hash", "type": "string", "required": true },
    { "name": "horse_name", "type": "string", "required": true },
    { "name": "horse_sex", "type": "string", "required": false },
    { "name": "foling_date", "type": "timestamp", "required": true },
    { "name": "foled_in", "type": "string", "required": true },
    { "name": "trainer_name", "type": "string", "required": true },
    { "name": "owner_name", "type": "string", "required": true },
    { "name": "breeder_name", "type": "string", "required": true },

    { "name": "bloodline_sire", "type": "string", "required": true },
    { "name": "bloodline_dam", "type": "string", "required": true },
    { "name": "bloodline_grandsire", "type": "string", "required": true },
    { "name": "bloodline_granddam", "type": "string", "required": true },
    { "name": "bloodline_maternal_grandsire", "type": "string", "required": true },
    { "name": "bloodline_maternal_granddam", "type": "string", "required": true },

    { "name": "race_hash", "type": "string", "required": true },
    { "name": "race_name", "type": "string", "required": true },
    { "name": "race_date", "type": "timestamp", "required": true },
    { "name": "race_number", "type": "int32", "required": true },
    { "name": "race_category", "type": "string", "required": true },
    { "name": "race_course", "type": "string", "required": true },
    { "name": "race_organization", "type": "string", "required": true },
    { "name": "race_is_only_female", "type": "bool", "required": true },
    { "name": "race_surface", "type": "string", "required": true },
    { "name": "race_distance", "type": "int32", "required": true },
    { "name": "race_direction", "type": "string", "required": true },
    { "name": "race_weather", "type": "string", "required": true },
    { "name": "race_condition", "type": "string", "required": true }
  ]
}
```

Table: race_info

```json
{
  "fields": [
    { "name": "id", "type": "string", "required": true },
    { "name": "race_hash", "type": "string", "required": true },
    { "name": "race_name", "type": "string", "required": true },
    { "name": "race_date", "type": "timestamp", "required": true },
    { "name": "race_number", "type": "int32", "required": true },
    { "name": "race_category", "type": "string", "required": true },
    { "name": "race_course", "type": "string", "required": true },
    { "name": "race_organization", "type": "string", "required": true },
    { "name": "race_is_only_female", "type": "bool", "required": true },
    { "name": "race_surface", "type": "string", "required": true },
    { "name": "race_distance", "type": "int32", "required": true },
    { "name": "race_direction", "type": "string", "required": true },
    { "name": "race_weather", "type": "string", "required": true },
    { "name": "race_condition", "type": "string", "required": true },

    {
      "name": "race_finishing_order",
      "type": "list",
      "required": true,
      "items": { "type": "int32" }
    },
    {
      "name": "race_result_all_corner_positions",
      "type": "list",
      "required": true,
      "items": { "type": "string" }
    },
    {
      "name": "race_furlong_splits",
      "type": "list",
      "required": true,
      "items": { "type": "float64" }
    },

    {
      "name": "ticket_tansho",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          { "name": "number", "type": "int32", "required": true },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_fukusho",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          { "name": "number", "type": "int32", "required": true },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_wakuren",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_umaren",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_wide",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_umatan",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_3renpuku",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    },
    {
      "name": "ticket_3rentan",
      "type": "list",
      "required": true,
      "items": {
        "type": "struct",
        "fields": [
          {
            "name": "numbers",
            "type": "list",
            "required": true,
            "items": { "type": "int32" }
          },
          { "name": "payout", "type": "int32", "required": true },
          { "name": "popularity_rank", "type": "int32", "required": true }
        ]
      }
    }
  ]
}
```
