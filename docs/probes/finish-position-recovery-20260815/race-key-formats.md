# `race_key` formats observed for 2026-08-15

Do not join the two families below directly on `race_key`.

| Family           | Format                                  | Example               | Tables observed on 2026-08-15                                                                                                                                                                                                       |
| ---------------- | --------------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Discovery/source | `{source}:{year}:{MMDD}:{venue}:{race}` | `jra:2026:0815:04:02` | `realtime_race_sources`, `jra_track_condition_snapshots`, `premium_data_top_horses`, `premium_paddock_content_hashes`, `premium_race_data_fetch_state`, `premium_race_links`, `premium_stable_comments`, `premium_training_reviews` |
| Inference        | `{source}:{YYYYMMDD}:{venue}:{race}`    | `jra:20260815:04:02`  | `running_style_inference_state`, `finish_position_inference_state`, `race_running_styles`                                                                                                                                           |
| Mixed logging    | either form                             | both forms observed   | `fetch_logs`                                                                                                                                                                                                                        |

Other source-side tables with a `race_key` foreign key to `realtime_race_sources` follow the discovery/source identity even if they had no rows for this date: `daily_race_entries`, `horse_weight_snapshots`, `odds_snapshots`, `premium_paddock_bulletins`, `premium_paddock_fetch_state`, `premium_paddock_notification_events`, `premium_paddock_notification_state`, `race_entry_snapshots`, and `race_result_snapshots`.

Neon `race_entry_corner_features`, `race_running_style_model_predictions`, and `race_finish_position_model_predictions` do not use a composite `race_key`; they expose source/date/venue/race columns separately.

## Safe join

Prefer the explicit columns:

```sql
... ON state.source = races.source
AND state.kaisai_nen = races.kaisai_nen
AND state.kaisai_tsukihi = races.kaisai_tsukihi
AND state.keibajo_code = races.keibajo_code
AND state.race_bango = races.race_bango
```

If only the strings are available, normalize both into explicit columns first. Do not remove colons blindly because that also destroys field boundaries.
