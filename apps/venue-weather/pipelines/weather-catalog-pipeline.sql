INSERT INTO venue_weather_hourly_sink
SELECT
  race_date,
  keibajo_code,
  weather_hour,
  weather_data_type,
  venue_name,
  latitude,
  longitude,
  weather_code,
  temperature,
  precipitation,
  wind_speed,
  wind_gusts,
  fetched_at
FROM venue_weather_ingest_stream
