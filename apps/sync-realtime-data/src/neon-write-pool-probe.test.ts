// Run with bun.
import { beforeEach, expect, it, vi } from "vitest";

import { classifyNeonWritePoolQueryError, probeNeonWritePool } from "./neon-write-pool-probe";
import type { Env } from "./types";

const getFinishPositionWritePoolMock = vi.hoisted(() => vi.fn());

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionWritePool: getFinishPositionWritePoolMock,
}));

const queryMock = vi.fn();

beforeEach(() => {
  getFinishPositionWritePoolMock.mockReset();
  queryMock.mockReset();
  getFinishPositionWritePoolMock.mockReturnValue({ query: queryMock });
});

it("reports a writable primary when both prediction tables accept insert", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  const result = await probeNeonWritePool({
    DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
  } as unknown as Env);

  expect(result).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
  expect(queryMock.mock.calls[0]?.[0]).toBe(
    "select pg_is_in_recovery() as in_recovery, current_setting('transaction_read_only') = 'on' as transaction_read_only, current_setting('default_transaction_read_only') = 'on' as default_transaction_read_only, to_regclass('public.race_running_style_model_predictions') is not null as rs_table_present, to_regclass('public.race_finish_position_model_predictions') is not null as fp_table_present, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'INSERT'), false)) as can_insert_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'INSERT'), false)) as can_insert_finish_position, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'SELECT'), false)) as can_select_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'SELECT'), false)) as can_select_finish_position, (to_regclass('public.race_running_style_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_running_style_model_predictions'), 'UPDATE'), false)) as can_update_running_style, (to_regclass('public.race_finish_position_model_predictions') is not null and coalesce(has_table_privilege(to_regclass('public.race_finish_position_model_predictions'), 'UPDATE'), false)) as can_update_finish_position",
  );
  expect(queryMock.mock.calls[0]?.length).toBe(1);
  expect(getFinishPositionWritePoolMock).toHaveBeenCalledTimes(1);
});

it("marks recovery replicas as not writable while keeping insert flags independent", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: true,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: true,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: false,
  });
});

it("marks a transaction_read_only session as not writable", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: true,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: true,
    writablePrimary: false,
  });
});

it("keeps a primary writable when default_transaction_read_only is on but the active transaction is writable", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: true,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: true,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports a missing running-style table without clearing finish-position flags", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: false,
        can_select_finish_position: true,
        can_select_running_style: false,
        can_update_finish_position: true,
        can_update_running_style: false,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: false,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: false,
    canSelectFinishPosition: true,
    canSelectRunningStyle: false,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: false,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: false,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports a missing finish-position table without clearing running-style flags", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: false,
        can_insert_running_style: true,
        can_select_finish_position: false,
        can_select_running_style: true,
        can_update_finish_position: false,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: false,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: false,
    canInsertRunningStyle: true,
    canSelectFinishPosition: false,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: false,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: false,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing insert privilege when both prediction tables exist", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: false,
        can_insert_running_style: false,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: false,
    canInsertRunningStyle: false,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing running-style insert privilege without clearing finish-position flags", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: false,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: false,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing finish-position insert privilege without clearing running-style flags", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: false,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: false,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("uses NEON_DATABASE_URL when DATABASE_URL_NEON is absent", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      NEON_DATABASE_URL: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "NEON_DATABASE_URL",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("prefers DATABASE_URL_NEON when both writable secrets are present", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
      NEON_DATABASE_URL: "postgres://probe-user:probe-secret@db.example/secondary",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("ignores Hyperdrive when DATABASE_URL_NEON is present", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
      HYPERDRIVE: { connectionString: "postgres://probe-user:probe-secret@hyperdrive.example/app" },
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
  expect(getFinishPositionWritePoolMock.mock.calls).toStrictEqual([
    [
      {
        DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
        HYPERDRIVE: {
          connectionString: "postgres://probe-user:probe-secret@hyperdrive.example/app",
        },
      },
    ],
  ]);
});

it("ignores Hyperdrive when only NEON_DATABASE_URL is present", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      HYPERDRIVE: { connectionString: "postgres://probe-user:probe-secret@hyperdrive.example/app" },
      NEON_DATABASE_URL: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "NEON_DATABASE_URL",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("classifies a missing write pool as unconfigured without querying", async () => {
  expect(await probeNeonWritePool({} as unknown as Env)).toStrictEqual({
    errorClass: "unconfigured",
    ok: false,
  });
  expect(getFinishPositionWritePoolMock).toHaveBeenCalledTimes(0);
  expect(queryMock).toHaveBeenCalledTimes(0);
});

it("classifies Hyperdrive-only env as unconfigured without querying", async () => {
  expect(
    await probeNeonWritePool({
      HYPERDRIVE: { connectionString: "postgres://probe-user:probe-secret@db.example/app" },
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unconfigured",
    ok: false,
  });
  expect(getFinishPositionWritePoolMock).toHaveBeenCalledTimes(0);
  expect(queryMock).toHaveBeenCalledTimes(0);
});

it("classifies empty writable secrets as unconfigured without querying", async () => {
  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "",
      HYPERDRIVE: { connectionString: "postgres://probe-user:probe-secret@db.example/app" },
      NEON_DATABASE_URL: "",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unconfigured",
    ok: false,
  });
  expect(getFinishPositionWritePoolMock).toHaveBeenCalledTimes(0);
  expect(queryMock).toHaveBeenCalledTimes(0);
});

it("treats a whitespace-only DATABASE_URL_NEON as configured and queries it", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: " ",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
  expect(getFinishPositionWritePoolMock).toHaveBeenCalledTimes(1);
  expect(queryMock).toHaveBeenCalledTimes(1);
});

it("returns only a safe auth class when the query error contains a DSN", async () => {
  const debugSpy = vi.spyOn(console, "debug").mockImplementation(() => undefined);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const infoSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  queryMock.mockRejectedValue({
    code: "28P01",
    message:
      "password authentication failed postgres://probe-user:probe-secret@db.example/app npg_fakeToken",
  });

  const result = await probeNeonWritePool({
    DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
  } as unknown as Env);
  const serialized: string = JSON.stringify(result);

  expect(result).toStrictEqual({
    errorClass: "auth",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
  expect(serialized.indexOf("postgres://probe-user:probe-secret@db.example/app") > -1).toBe(false);
  expect(serialized.indexOf("probe-secret") > -1).toBe(false);
  expect(serialized.indexOf("npg_fakeToken") > -1).toBe(false);
  expect(debugSpy.mock.calls).toStrictEqual([]);
  expect(errorSpy.mock.calls).toStrictEqual([]);
  expect(infoSpy.mock.calls).toStrictEqual([]);
  expect(logSpy.mock.calls).toStrictEqual([]);
  expect(warnSpy.mock.calls).toStrictEqual([]);
  debugSpy.mockRestore();
  errorSpy.mockRestore();
  infoSpy.mockRestore();
  logSpy.mockRestore();
  warnSpy.mockRestore();
});

it("returns only a safe network class when the query error contains a host", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  queryMock.mockRejectedValue({
    code: "ECONNREFUSED",
    message: "connect ECONNREFUSED postgres://probe-user:probe-secret@db.example:5432/app",
  });

  const result = await probeNeonWritePool({
    NEON_DATABASE_URL: "postgres://probe-user:probe-secret@db.example/app",
  } as unknown as Env);

  expect(result).toStrictEqual({
    errorClass: "network",
    ok: false,
    source: "NEON_DATABASE_URL",
  });
  expect(errorSpy.mock.calls).toStrictEqual([]);
  errorSpy.mockRestore();
});

it("classifies SQLSTATE 28P01 as auth", () => {
  expect(classifyNeonWritePoolQueryError({ code: "28P01" })).toBe("auth");
});

it("classifies a lowercase SQLSTATE 28p01 as auth", () => {
  expect(classifyNeonWritePoolQueryError({ code: "28p01" })).toBe("auth");
});

it("classifies a password authentication failed message as auth", () => {
  expect(
    classifyNeonWritePoolQueryError(
      new Error("password authentication failed for user probe-user"),
    ),
  ).toBe("auth");
});

it("classifies SQLSTATE 28000 as auth", () => {
  expect(classifyNeonWritePoolQueryError({ code: "28000" })).toBe("auth");
});

it("classifies a pg_hba message as auth", () => {
  expect(classifyNeonWritePoolQueryError(new Error("no pg_hba.conf entry for host"))).toBe("auth");
});

it("classifies a SASL message as auth", () => {
  expect(classifyNeonWritePoolQueryError(new Error("SASL authentication failed"))).toBe("auth");
});

it("classifies a SCRAM message as auth", () => {
  expect(classifyNeonWritePoolQueryError(new Error("SCRAM authentication failed"))).toBe("auth");
});

it("classifies an invalid authorization message as auth", () => {
  expect(classifyNeonWritePoolQueryError(new Error("invalid authorization specification"))).toBe(
    "auth",
  );
});

it("classifies ECONNREFUSED as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "ECONNREFUSED" })).toBe("network");
});

it("classifies ETIMEDOUT as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "ETIMEDOUT" })).toBe("network");
});

it("classifies ECONNRESET as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "ECONNRESET" })).toBe("network");
});

it("classifies ENOTFOUND as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "ENOTFOUND" })).toBe("network");
});

it("classifies EAI_AGAIN as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "EAI_AGAIN" })).toBe("network");
});

it("classifies ENETUNREACH as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "ENETUNREACH" })).toBe("network");
});

it("classifies EHOSTUNREACH as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "EHOSTUNREACH" })).toBe("network");
});

it("classifies EPIPE as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "EPIPE" })).toBe("network");
});

it("classifies SQLSTATE 08001 as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "08001" })).toBe("network");
});

it("classifies SQLSTATE 08004 as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "08004" })).toBe("network");
});

it("classifies SQLSTATE 08006 as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "08006" })).toBe("network");
});

it("classifies SQLSTATE 08007 as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "08007" })).toBe("network");
});

it("classifies SQLSTATE 57P03 as network", () => {
  expect(classifyNeonWritePoolQueryError({ code: "57P03" })).toBe("network");
});

it("classifies errno ECONNREFUSED as network when code is absent", () => {
  expect(classifyNeonWritePoolQueryError({ errno: "ECONNREFUSED" })).toBe("network");
});

it("classifies errno when code is empty", () => {
  expect(classifyNeonWritePoolQueryError({ code: "", errno: "ETIMEDOUT" })).toBe("network");
});

it("classifies a lowercase econnrefused message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("connect econnrefused 127.0.0.1"))).toBe(
    "network",
  );
});

it("classifies an etimedout message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("connect etimedout db.example:5432"))).toBe(
    "network",
  );
});

it("classifies an enotfound message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("getaddrinfo enotfound db.example"))).toBe(
    "network",
  );
});

it("classifies an eai_again message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("getaddrinfo eai_again db.example"))).toBe(
    "network",
  );
});

it("classifies an econnreset message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("read econnreset"))).toBe("network");
});

it("classifies an enetunreach message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("connect enetunreach"))).toBe("network");
});

it("classifies an ehostunreach message as network", () => {
  expect(classifyNeonWritePoolQueryError(new Error("connect ehostunreach"))).toBe("network");
});

it("classifies SQLSTATE 25006 as read_only", () => {
  expect(classifyNeonWritePoolQueryError({ code: "25006" })).toBe("read_only");
});

it("classifies a read-only transaction message as read_only", () => {
  expect(
    classifyNeonWritePoolQueryError(new Error("cannot execute INSERT in a read-only transaction")),
  ).toBe("read_only");
});

it("returns read_only without collapsing a successful recovery probe", async () => {
  queryMock.mockRejectedValue({
    code: "25006",
    message: "cannot execute INSERT in a read-only transaction",
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "read_only",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies SQLSTATE 42501 as unknown", () => {
  expect(classifyNeonWritePoolQueryError({ code: "42501" })).toBe("unknown");
});

it("classifies SQLSTATE 53300 as unknown", () => {
  expect(classifyNeonWritePoolQueryError({ code: "53300" })).toBe("unknown");
});

it("classifies a generic Error as unknown", () => {
  expect(classifyNeonWritePoolQueryError(new Error("boom"))).toBe("unknown");
});

it("classifies null as unknown", () => {
  expect(classifyNeonWritePoolQueryError(null)).toBe("unknown");
});

it("classifies a non-string error message as unknown", () => {
  expect(classifyNeonWritePoolQueryError({ message: 42 })).toBe("unknown");
});

it("classifies an empty errno as unknown when message is absent", () => {
  expect(classifyNeonWritePoolQueryError({ code: "", errno: "" })).toBe("unknown");
});

it("classifies a numeric code as unknown when message is absent", () => {
  expect(classifyNeonWritePoolQueryError({ code: 28 })).toBe("unknown");
});

it("classifies a numeric errno as unknown when message is absent", () => {
  expect(classifyNeonWritePoolQueryError({ errno: 111 })).toBe("unknown");
});

it("classifies a lowercase errno as network", () => {
  expect(classifyNeonWritePoolQueryError({ errno: "econnrefused" })).toBe("network");
});

it("keeps an unknown SQLSTATE as unknown even when the message looks like auth", () => {
  expect(
    classifyNeonWritePoolQueryError({
      code: "42501",
      message: "password authentication failed for user probe-user",
    }),
  ).toBe("unknown");
});

it("classifies an epipe message without a code as unknown", () => {
  expect(classifyNeonWritePoolQueryError(new Error("write epipe"))).toBe("unknown");
});

it("classifies a pg Error instance code as auth before the message", () => {
  expect(
    classifyNeonWritePoolQueryError(
      Object.assign(new Error("password authentication failed"), { code: "28P01" }),
    ),
  ).toBe("auth");
});

it("prefers SQLSTATE auth when the message also contains a DSN and ECONNREFUSED", () => {
  expect(
    classifyNeonWritePoolQueryError({
      code: "28P01",
      message:
        "password authentication failed postgres://probe-user:probe-secret@db.example/app ECONNREFUSED npg_fakeToken",
    }),
  ).toBe("auth");
});

it("classifies an empty result set as unknown", async () => {
  queryMock.mockResolvedValue({ rows: [] });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean recovery flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: "yes",
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean running-style insert flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: 1,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean finish-position insert flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: 1,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean default_transaction_read_only flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: "on",
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean fp_table_present flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: "yes",
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean rs_table_present flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: "yes",
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean transaction_read_only flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: "on",
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a missing probe row as unknown", async () => {
  queryMock.mockResolvedValue({ rows: [null] });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a malformed query result as unknown", async () => {
  queryMock.mockResolvedValue({ rows: "bad" });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-object query result as unknown", async () => {
  queryMock.mockResolvedValue(null);

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a thrown non-Error query failure as unknown", async () => {
  queryMock.mockRejectedValue("boom");

  expect(
    await probeNeonWritePool({
      NEON_DATABASE_URL: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "NEON_DATABASE_URL",
  });
});

it("classifies a probe row missing in_recovery as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a query result without rows as unknown", async () => {
  queryMock.mockResolvedValue({});

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a string probe row as unknown", async () => {
  queryMock.mockResolvedValue({ rows: ["bad"] });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies an undefined probe row as unknown", async () => {
  queryMock.mockResolvedValue({ rows: [undefined] });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("runs a non-mutating SELECT with no bind parameters", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  await probeNeonWritePool({
    DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
  } as unknown as Env);

  const sql: unknown = queryMock.mock.calls[0]?.[0];
  expect(queryMock.mock.calls[0]?.length).toBe(1);
  expect(typeof sql).toBe("string");
  expect(typeof sql === "string" && sql.startsWith("select ")).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("insert into") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("update ") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("delete ") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("truncate") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("alter ") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("drop ") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("create ") > -1).toBe(false);
  expect(
    typeof sql === "string" && sql.indexOf("public.race_running_style_model_predictions") > -1,
  ).toBe(true);
  expect(
    typeof sql === "string" && sql.indexOf("public.race_finish_position_model_predictions") > -1,
  ).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("pg_is_in_recovery()") > -1).toBe(true);
  expect(
    typeof sql === "string" && sql.indexOf("current_setting('transaction_read_only')") > -1,
  ).toBe(true);
  expect(
    typeof sql === "string" && sql.indexOf("current_setting('default_transaction_read_only')") > -1,
  ).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("to_regclass(") > -1).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("has_table_privilege(") > -1).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("'INSERT'") > -1).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("'SELECT'") > -1).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("'UPDATE'") > -1).toBe(true);
  expect(typeof sql === "string" && sql.indexOf("'INSERT, UPDATE'") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("'SELECT,INSERT,UPDATE'") > -1).toBe(false);
  expect(typeof sql === "string" && sql.indexOf("prepare") > -1).toBe(false);
});

it("reports missing running-style update privilege without clearing finish-position upsert", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: false,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: false,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing finish-position update privilege without clearing running-style upsert", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: false,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: false,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing running-style select privilege without clearing finish-position upsert", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: false,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: false,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing finish-position select privilege without clearing running-style upsert", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: false,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: false,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("reports missing select privilege as blocking upsert on both prediction tables", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: false,
        can_select_running_style: false,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: false,
    canSelectRunningStyle: false,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: false,
    canUpsertRunningStyle: false,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: false,
    writablePrimary: true,
  });
});

it("marks a default-read-only primary as not writable when the active transaction is also read-only", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: true,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: true,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: true,
    fpTablePresent: true,
    inRecovery: false,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: true,
    writablePrimary: false,
  });
});

it("marks a live replica as not writable while keeping upsert privilege flags independent", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: true,
        rs_table_present: true,
        transaction_read_only: true,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    canInsertFinishPosition: true,
    canInsertRunningStyle: true,
    canSelectFinishPosition: true,
    canSelectRunningStyle: true,
    canUpdateFinishPosition: true,
    canUpdateRunningStyle: true,
    canUpsertFinishPosition: true,
    canUpsertRunningStyle: true,
    defaultTransactionReadOnly: false,
    fpTablePresent: true,
    inRecovery: true,
    ok: true,
    rsTablePresent: true,
    source: "DATABASE_URL_NEON",
    transactionReadOnly: true,
    writablePrimary: false,
  });
});

it("classifies a non-boolean running-style update flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: 1,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean finish-position update flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: true,
        can_update_finish_position: 1,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean running-style select flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: true,
        can_select_running_style: 1,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});

it("classifies a non-boolean finish-position select flag as unknown", async () => {
  queryMock.mockResolvedValue({
    rows: [
      {
        can_insert_finish_position: true,
        can_insert_running_style: true,
        can_select_finish_position: 1,
        can_select_running_style: true,
        can_update_finish_position: true,
        can_update_running_style: true,
        default_transaction_read_only: false,
        fp_table_present: true,
        in_recovery: false,
        rs_table_present: true,
        transaction_read_only: false,
      },
    ],
  });

  expect(
    await probeNeonWritePool({
      DATABASE_URL_NEON: "postgres://probe-user:probe-secret@db.example/app",
    } as unknown as Env),
  ).toStrictEqual({
    errorClass: "unknown",
    ok: false,
    source: "DATABASE_URL_NEON",
  });
});
