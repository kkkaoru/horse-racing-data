import { expect, it } from "vitest";
import {
  assertNeonArgsSafe,
  buildCopyPipelineBashCommand,
  buildNeonContainerPsqlArgs,
  buildNeonPsqlEnv,
  neonArgvLeaksSecret,
  neonLibpqEnvKeys,
  parseNeonLibpqEnv,
  resolveNeonConnection,
} from "./push-neon-sync";

it("throws when NEON_DIRECT_DATABASE_URL is missing", () => {
  expect(() => resolveNeonConnection({})).toThrow("NEON_DIRECT_DATABASE_URL is required");
});

it("throws when NEON_DIRECT_DATABASE_URL is empty", () => {
  expect(() => resolveNeonConnection({ NEON_DIRECT_DATABASE_URL: "" })).toThrow(
    "NEON_DIRECT_DATABASE_URL is required",
  );
});

it("defaults to the long-lived Apple PostgreSQL container", () => {
  expect(
    resolveNeonConnection({
      NEON_DIRECT_DATABASE_URL: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    }),
  ).toStrictEqual({
    dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    containerName: "horse-racing-local-postgresql",
  });
});

it("uses a custom neon psql container name", () => {
  expect(
    resolveNeonConnection({
      NEON_DIRECT_DATABASE_URL: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
      REPLICA_SYNC_NEON_PSQL_CONTAINER: "my-psql-sidecar",
    }),
  ).toStrictEqual({
    dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    containerName: "my-psql-sidecar",
  });
});

it("falls back to the default container when the override is empty", () => {
  expect(
    resolveNeonConnection({
      NEON_DIRECT_DATABASE_URL: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
      REPLICA_SYNC_NEON_PSQL_CONTAINER: "",
    }).containerName,
  ).toBe("horse-racing-local-postgresql");
});

it("throws when the neon dsn is not a url", () => {
  expect(() => parseNeonLibpqEnv("::::")).toThrow(
    "NEON_DIRECT_DATABASE_URL must be a valid postgres URL",
  );
});

it("throws when the neon dsn protocol is not postgres", () => {
  expect(() => parseNeonLibpqEnv("https://example.test/neondb")).toThrow(
    "NEON_DIRECT_DATABASE_URL must be a postgres URL",
  );
});

it("throws when the neon dsn is missing a host", () => {
  expect(() => parseNeonLibpqEnv("postgresql:///neondb")).toThrow(
    "NEON_DIRECT_DATABASE_URL must include a host",
  );
});

it("throws when the neon dsn is missing a database name", () => {
  expect(() =>
    parseNeonLibpqEnv("postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech"),
  ).toThrow("NEON_DIRECT_DATABASE_URL must include a database name");
});

it("parses libpq env from a neon dsn without putting the password in argv keys", () => {
  expect(
    parseNeonLibpqEnv(
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech:5432/neondb?sslmode=require&channel_binding=require&keepalives=1&keepalives_idle=60&keepalives_interval=30&keepalives_count=3",
    ),
  ).toStrictEqual({
    PGHOST: "ep.example.neon.tech",
    PGPORT: "5432",
    PGUSER: "neondb_owner",
    PGDATABASE: "neondb",
    PGPASSWORD: "s3cret-pass",
    PGSSLMODE: "require",
    PGCHANNELBINDING: "require",
    PGKEEPALIVES: "1",
    PGKEEPALIVESIDLE: "60",
    PGKEEPALIVESINTERVAL: "30",
    PGKEEPALIVESCOUNT: "3",
  });
});

it("omits optional libpq env keys when the dsn does not include them", () => {
  expect(parseNeonLibpqEnv("postgresql://neondb_owner@ep.example.neon.tech/neondb")).toStrictEqual({
    PGHOST: "ep.example.neon.tech",
    PGUSER: "neondb_owner",
    PGDATABASE: "neondb",
  });
});

it("decodes a percent-encoded neon password into PGPASSWORD", () => {
  expect(
    parseNeonLibpqEnv("postgresql://neondb_owner:s3cret%40pass@ep.example.neon.tech/neondb")
      .PGPASSWORD,
  ).toBe("s3cret@pass");
});

it("keeps an undecodable password token as-is", () => {
  expect(
    parseNeonLibpqEnv("postgresql://neondb_owner:100%match@ep.example.neon.tech/neondb").PGPASSWORD,
  ).toBe("100%match");
});

it("ignores empty neon query parameters", () => {
  expect(
    parseNeonLibpqEnv("postgresql://neondb_owner@ep.example.neon.tech/neondb?sslmode=").PGSSLMODE,
  ).toBe(undefined);
});

it("returns libpq keys in a stable order", () => {
  expect(
    neonLibpqEnvKeys({
      PGDATABASE: "neondb",
      PGPASSWORD: "s3cret-pass",
      PGHOST: "ep.example.neon.tech",
      PGUSER: "neondb_owner",
    }),
  ).toStrictEqual(["PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD"]);
});

it("puts neon credentials only in libpq env", () => {
  expect(
    buildNeonPsqlEnv({
      dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb?sslmode=require",
      containerName: "horse-racing-local-postgresql",
    }),
  ).toStrictEqual({
    PGHOST: "ep.example.neon.tech",
    PGUSER: "neondb_owner",
    PGDATABASE: "neondb",
    PGPASSWORD: "s3cret-pass",
    PGSSLMODE: "require",
  });
});

it("builds container exec args that inherit libpq env without putting secrets in argv", () => {
  expect(
    buildNeonContainerPsqlArgs({
      connection: {
        dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb?sslmode=require",
        containerName: "horse-racing-local-postgresql",
      },
    }),
  ).toStrictEqual([
    "exec",
    "-e",
    "PGHOST",
    "-e",
    "PGUSER",
    "-e",
    "PGDATABASE",
    "-e",
    "PGPASSWORD",
    "-e",
    "PGSSLMODE",
    "-i",
    "horse-racing-local-postgresql",
    "sh",
    "-c",
    'exec psql "$@"',
    "psql",
  ]);
});

it("appends extra psql args after the dummy sh $0", () => {
  expect(
    buildNeonContainerPsqlArgs({
      connection: {
        dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
        containerName: "horse-racing-local-postgresql",
      },
      extraArgs: ["-v", "ON_ERROR_STOP=1", "-qAtc", "select 1"],
    }),
  ).toStrictEqual([
    "exec",
    "-e",
    "PGHOST",
    "-e",
    "PGUSER",
    "-e",
    "PGDATABASE",
    "-e",
    "PGPASSWORD",
    "-i",
    "horse-racing-local-postgresql",
    "sh",
    "-c",
    'exec psql "$@"',
    "psql",
    "-v",
    "ON_ERROR_STOP=1",
    "-qAtc",
    "select 1",
  ]);
});

it("does not leak the neon password through neon container argv", () => {
  expect(
    neonArgvLeaksSecret(
      buildNeonContainerPsqlArgs({
        connection: {
          dsn: "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
          containerName: "horse-racing-local-postgresql",
        },
      }),
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).toBe(false);
});

it("detects a full dsn embedded in argv", () => {
  expect(
    neonArgvLeaksSecret(
      ["exec", "-i", "psql", "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb"],
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).toBe(true);
});

it("detects a password token embedded in argv", () => {
  expect(
    neonArgvLeaksSecret(
      ["psql", "s3cret-pass"],
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).toBe(true);
});

it("detects a percent-encoded password token embedded in argv", () => {
  expect(
    neonArgvLeaksSecret(
      ["psql", "s3cret%40pass"],
      "postgresql://neondb_owner:s3cret%40pass@ep.example.neon.tech/neondb",
    ),
  ).toBe(true);
});

it("does not treat a passwordless dsn as a leak", () => {
  expect(
    neonArgvLeaksSecret(
      ["psql", "-h", "ep.example.neon.tech"],
      "postgresql://neondb_owner@ep.example.neon.tech/neondb",
    ),
  ).toBe(false);
});

it("returns false when the dsn is not a url and argv has no secret substring", () => {
  expect(neonArgvLeaksSecret(["psql"], "::::")).toBe(false);
});

it("returns true when a non-url dsn string itself appears in argv", () => {
  expect(neonArgvLeaksSecret(["::::"], "::::")).toBe(true);
});

it("accepts neon argv that only inherit libpq env keys", () => {
  expect(() =>
    assertNeonArgsSafe(
      [
        "exec",
        "-e",
        "PGHOST",
        "-e",
        "PGPASSWORD",
        "-i",
        "horse-racing-local-postgresql",
        "sh",
        "-c",
        'exec psql "$@"',
        "psql",
      ],
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).not.toThrow();
});

it("rejects neon argv that embed the password", () => {
  expect(() =>
    assertNeonArgsSafe(
      ["exec", "-i", "psql", "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb"],
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).toThrow("Neon psql argv must not contain the database password");
});

it("builds a copy pipeline that inherits libpq env only inside the container", () => {
  expect(buildCopyPipelineBashCommand(["PGHOST", "PGPASSWORD"])).toStrictEqual(
    `container exec "$LOCAL_CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F "$(printf '\\t')" -c "$LOCAL_COPY_SQL" | container exec -e PGHOST -e PGPASSWORD -i "$NEON_PSQL_CONTAINER" sh -c 'exec psql "$@"' psql -v ON_ERROR_STOP=1 -c "$NEON_COPY_SQL"`,
  );
});

it("does not leak the neon password through the copy pipeline bash string", () => {
  expect(
    neonArgvLeaksSecret(
      [buildCopyPipelineBashCommand(["PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD"])],
      "postgresql://neondb_owner:s3cret-pass@ep.example.neon.tech/neondb",
    ),
  ).toBe(false);
});
