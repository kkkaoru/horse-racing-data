// Polling window gate. Historically restricted odds polling to JST 06-21 to
// throttle D1 read pressure, but the per-race cadence + KV race-list cache +
// enqueue-lock TTLs already throttle the planner. Multi-day populate enqueues
// tomorrow's races into odds_fetch_state during the previous evening, so the
// planner must run around the clock; otherwise late-night planner ticks would
// skip the next day's JRA races entirely. Now always-on.
export const shouldRunOddsCron = (): boolean => true;
