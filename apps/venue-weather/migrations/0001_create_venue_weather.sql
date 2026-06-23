CREATE TABLE IF NOT EXISTS venue_weather (
    keibajo_code    TEXT    NOT NULL,
    race_date       TEXT    NOT NULL,
    weather_hour    INTEGER NOT NULL,
    weather_type    TEXT    NOT NULL,
    venue_name      TEXT    NOT NULL,
    latitude        REAL    NOT NULL,
    longitude       REAL    NOT NULL,
    weather_code    INTEGER,
    temperature     REAL,
    precipitation   REAL,
    wind_speed      REAL,
    wind_gusts      REAL,
    fetched_at      TEXT    NOT NULL,
    PRIMARY KEY (keibajo_code, race_date, weather_hour, weather_type)
);

CREATE INDEX IF NOT EXISTS idx_venue_weather_date_venue
    ON venue_weather (race_date, keibajo_code);
