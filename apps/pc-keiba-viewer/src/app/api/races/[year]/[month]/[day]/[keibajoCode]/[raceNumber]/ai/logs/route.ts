import { NextResponse } from "next/server";

import { getRaceSourceByRoute } from "../../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../../lib/codes";

export const dynamic = "force-dynamic";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

interface RaceAiServerCommand {
  action: "reset";
  createdAt: string;
  id: string;
  raceKey: string;
}

type RaceAiServerCommandStore = Map<string, RaceAiServerCommand>;

declare global {
  var pcKeibaRaceAiServerCommandStore: RaceAiServerCommandStore | undefined;
}

const getStore = (): RaceAiServerCommandStore => {
  globalThis.pcKeibaRaceAiServerCommandStore ??= new Map<string, RaceAiServerCommand>();
  return globalThis.pcKeibaRaceAiServerCommandStore;
};

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const isValidParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/u.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const buildRaceKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}): string => `${source}:${year}${month}${day}:${keibajoCode}:${raceNumber}`;

const resolveRaceKey = async (request: Request, context: RouteContext): Promise<string | null> => {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  if (!isValidParams(year, month, day, keibajoCode, raceNumber)) {
    return null;
  }
  const sourceParam = new URL(request.url).searchParams.get("source");
  if (sourceParam && !isRaceSource(sourceParam)) {
    return null;
  }
  const source = isRaceSource(sourceParam)
    ? sourceParam
    : await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  return source ? buildRaceKey({ day, keibajoCode, month, raceNumber, source, year }) : null;
};

export async function GET(request: Request, context: RouteContext) {
  const raceKey = await resolveRaceKey(request, context);
  if (!raceKey) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  return NextResponse.json({
    command: getStore().get(raceKey) ?? null,
    raceKey,
  });
}

export async function POST(request: Request, context: RouteContext) {
  const raceKey = await resolveRaceKey(request, context);
  if (!raceKey) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const payload: unknown = await request.json().catch(() => ({}));
  const ackCommandId =
    isRecord(payload) && typeof payload.ackCommandId === "string" ? payload.ackCommandId : null;
  const store = getStore();
  const current = store.get(raceKey);
  if (ackCommandId) {
    if (current?.id === ackCommandId) {
      store.delete(raceKey);
    }
    return NextResponse.json({
      command: store.get(raceKey) ?? null,
      ok: true,
      raceKey,
    });
  }

  const action = isRecord(payload) && payload.action === "reset" ? payload.action : "reset";
  const command: RaceAiServerCommand = {
    action,
    createdAt: new Date().toISOString(),
    id: crypto.randomUUID(),
    raceKey,
  };
  store.set(raceKey, command);
  return NextResponse.json({ command, ok: true, raceKey });
}

export async function DELETE(request: Request, context: RouteContext) {
  return POST(
    new Request(request.url, {
      body: JSON.stringify({ action: "reset" }),
      headers: { "content-type": "application/json" },
      method: "POST",
    }),
    context,
  );
}
