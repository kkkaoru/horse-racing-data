# A8 main generation is not today’s weight path (2026-08-16)

Confirmed from `a8-main-generation-20260816.md` +
`generate_a8_main.py`. No deploy.

A8 `A8/04` posts **22:50**. Window **21:00–22:00**. One local
Python command. Writes **tmp JSON only**. Explicitly **no**
PostgreSQL / Neon / R2 / cache / queue / container.

It rescores saved market-null vectors with
`jra-cb-v9-sim-2013-CLEAN`. It does **not** call
`triggerRescoreAfterWeights`, `PREDICT_QUEUE`, or
`processContainerPerRaceRescore`.

Today’s 65 / 227 min dwell does **not** apply. Do not move
21:00 earlier for that reason. Oversea owns execute.

If someone later wires A8 into the domestic weight queue, that
is a new design. It is not tonight’s command.
