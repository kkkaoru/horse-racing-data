import { expect, it, vi } from "vitest";
import {
  buildPsqlCommand,
  executeLocalSql,
  type PsqlCommandRunner,
} from "./scrape-netkeiba-training";

it("builds an Apple Container psql command with fail-fast transaction settings", () => {
  expect(
    buildPsqlCommand({
      containerName: "horse-racing-local-postgresql",
      database: "horse_racing",
      user: "horse_racing",
    }),
  ).toStrictEqual([
    "container",
    "exec",
    "-i",
    "horse-racing-local-postgresql",
    "psql",
    "-X",
    "-v",
    "ON_ERROR_STOP=1",
    "-U",
    "horse_racing",
    "-d",
    "horse_racing",
    "-Atq",
  ]);
});

it("passes SQL to the injected psql runner", async () => {
  const runner = vi.fn<PsqlCommandRunner>().mockResolvedValue("row\n");
  expect(
    await executeLocalSql(
      "select 1;",
      {
        containerName: "local-pg",
        database: "racing",
        user: "runner",
      },
      runner,
    ),
  ).toBe("row\n");
  expect(runner.mock.calls).toStrictEqual([
    [
      {
        command: [
          "container",
          "exec",
          "-i",
          "local-pg",
          "psql",
          "-X",
          "-v",
          "ON_ERROR_STOP=1",
          "-U",
          "runner",
          "-d",
          "racing",
          "-Atq",
        ],
        sql: "select 1;",
      },
    ],
  ]);
});
