// Runs with Bun for tests and in the browser behind the existing Access origin.
"use client";

import {
  useEffect,
  useId,
  useState,
  type ChangeEvent,
  type KeyboardEvent,
  type ReactNode,
} from "react";

import {
  BET_BUDGET_STEP,
  BET_COLUMNS_STORAGE_KEY,
  BET_REQUIRE_ALL_STORAGE_KEY,
  BET_TYPES,
  BET_TARGET_STORAGE_KEY,
  BET_TYPES_STORAGE_KEY,
  OPTIONAL_BET_COLUMNS,
  DEFAULT_BET_BUDGET,
  DEFAULT_BET_TARGET,
  MAX_BET_BUDGET,
  MAX_BET_TARGET,
  MIN_BET_BUDGET,
  MIN_BET_TARGET,
  betStyleLabel,
  betTypeLabel,
  buildClefBetsUrl,
  describePlan,
  formatPlanOdds,
  formatPlanPayout,
  formatPlanSetProfit,
  formatPlanStake,
  formatYen,
  normalizeBudget,
  normalizeTarget,
  planGroups,
  readClefBets,
  readHiddenBetColumns,
  readSelectedBetTypes,
  readStoredRequireAll,
  readStoredTarget,
  requestedBetTypes,
  summarizePlanSet,
  trimName,
  type ClefBetCard,
  type ClefBetRunner,
  type ClefBetSuggestion,
} from "../../../lib/clef-bets";
import type { ClefRerankRaceAddress } from "../../../lib/clef-rerank";
import { FrameNumberBadge, PlainHorseNumberBadge } from "./frame-number-badge";
import { useRealtimeRaceSelector } from "./realtime-client";

// The Worker found odds but no ticket that reaches the target within the budget.
const UNREACHABLE = "target-unreachable";
// With requireAll the budget and target cannot fit every checked type with odds.
const REQUIRED_UNFIT = "required-types-unfit";

interface ClefBetState {
  url: string;
  card: ClefBetCard | null | typeof UNREACHABLE | typeof REQUIRED_UNFIT;
}

interface BetBodyProps {
  state: ClefBetState | null;
  hidden: readonly string[];
  busy: boolean;
}

interface ColumnTogglesProps {
  hidden: readonly string[];
  onToggle: (label: string, visible: boolean) => void;
}

interface BetTypeTogglesProps {
  selected: readonly string[];
  available: readonly string[];
  onToggle: (type: string, checked: boolean) => void;
  requireAll: boolean;
  onRequireAll: (checked: boolean) => void;
}

interface GroupCellProps {
  plan: ClefBetSuggestion;
  runners: ReadonlyMap<number, ClefBetRunner>;
  pick: (runner: ClefBetRunner) => ReactNode;
}

interface BudgetControlProps {
  budget: number;
  onChange: (budget: number) => void;
}

interface TargetControlProps {
  target: number;
  onChange: (target: number) => void;
}

interface PickColumn {
  label: string;
  pick: (runner: ClefBetRunner) => ReactNode;
}

const SLIDER_MAX_BUDGET: number = 20_000;
const LARGE_STEP: number = 1000;
const FETCH_DELAY_MS: number = 400;
const MISSING: string = "-";
const MOBILE_QUERY: string = "(max-width: 760px)";
const FRAME_COLUMN: string = "枠番";
const REQUIRE_ALL_LABEL: string = "チェックした馬券の種類をすべて含める";
const FRAME_BET_TYPES: ReadonlySet<string> = new Set(["wakuren", "wakufuku", "wakutan"]);

// Multi-horse structures (軸, マルチ, フォーメーション, ボックス) only show frames for frame bets.
const hidesFrame = (column: PickColumn, plan: ClefBetSuggestion): boolean =>
  column.label === FRAME_COLUMN && plan.style !== "single" && !FRAME_BET_TYPES.has(plan.betType);
const PICK_COLUMNS: readonly PickColumn[] = [
  { label: "枠番", pick: (runner) => <FrameNumberBadge value={String(runner.frameNumber)} /> },
  {
    label: "馬番",
    pick: (runner) => <PlainHorseNumberBadge horseNumber={String(runner.horseNumber)} />,
  },
  { label: "馬名", pick: (runner) => trimName(runner.horseName) },
  { label: "騎手", pick: (runner) => trimName(runner.jockey) },
  { label: "調教師", pick: (runner) => trimName(runner.trainer) },
  { label: "馬主", pick: (runner) => trimName(runner.owner) },
];

const readReason = (body: unknown): ClefBetState["card"] => {
  const reason = typeof body === "object" && body !== null && "reason" in body ? body.reason : null;
  return reason === UNREACHABLE || reason === REQUIRED_UNFIT ? reason : null;
};

const loadCard = async (url: string, signal: AbortSignal): Promise<ClefBetState["card"]> => {
  try {
    const response = await fetch(url, { credentials: "same-origin", signal, cache: "no-store" });
    const body: unknown = await response.json();
    if (response.ok) return readClefBets(body);
    return readReason(body);
  } catch {
    return null;
  }
};

// One block per leg (軸 / 相手, 1着 / 2着 / 3着, ボックス) so nagashi, multi and formation
// tickets read as a betting slip instead of one row per combination.
function GroupCell({ plan, runners, pick }: GroupCellProps) {
  return (
    <td>
      {planGroups(plan).map((group) => (
        <div className="clef-bet-leg" key={group.label}>
          {group.label === "" ? null : <span className="clef-bet-leg-label">{group.label}</span>}
          {group.horses.map((horse) => {
            const runner = runners.get(horse);
            return <div key={horse}>{runner === undefined ? MISSING : pick(runner)}</div>;
          })}
        </div>
      ))}
    </td>
  );
}

// Same control as the 競走成績 filters; the choice is kept on this device for every race.
function ColumnToggles({ hidden, onToggle }: ColumnTogglesProps) {
  return (
    <fieldset className="clef-bet-columns">
      <legend>表示する列</legend>
      {OPTIONAL_BET_COLUMNS.map((label) => (
        <label className="race-results-checkbox-label" key={label}>
          <span>{label}</span>
          <span className="race-results-checkbox-control">
            <input
              aria-label={label}
              checked={!hidden.includes(label)}
              onChange={(event) => onToggle(label, event.currentTarget.checked)}
              type="checkbox"
            />
          </span>
        </label>
      ))}
    </fieldset>
  );
}

// Types without odds for this race stay visible but cannot be chosen.
function BetTypeToggles({
  selected,
  available,
  onToggle,
  requireAll,
  onRequireAll,
}: BetTypeTogglesProps) {
  return (
    <fieldset className="clef-bet-columns">
      <legend>馬券の種類</legend>
      {BET_TYPES.map((type) => {
        const enabled = available.includes(type);
        const label = betTypeLabel(type);
        return (
          <label className="race-results-checkbox-label" key={type}>
            <span>
              {label}
              {enabled ? null : "（オッズなし）"}
            </span>
            <span className="race-results-checkbox-control">
              <input
                aria-label={enabled ? label : `${label}（オッズなし）`}
                checked={enabled && selected.includes(type)}
                disabled={!enabled}
                onChange={(event) => onToggle(type, event.currentTarget.checked)}
                type="checkbox"
              />
            </span>
          </label>
        );
      })}
      <label className="race-results-checkbox-label">
        <span>{REQUIRE_ALL_LABEL}</span>
        <span className="race-results-checkbox-control">
          <input
            aria-label={REQUIRE_ALL_LABEL}
            checked={requireAll}
            onChange={(event) => onRequireAll(event.currentTarget.checked)}
            type="checkbox"
          />
        </span>
      </label>
    </fieldset>
  );
}

const readStored = (key: string): string | null => {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
};

const writeStored = (key: string, values: readonly string[] | number | boolean): void => {
  try {
    window.localStorage.setItem(key, JSON.stringify(values));
  } catch {
    // Private mode or blocked storage only loses the remembered choice.
  }
};

// Keeps the last answer on screen, dimmed, while a changed request is pending.
function BetBody({ state, hidden, busy }: BetBodyProps) {
  if (state === null) return <p>推奨馬券を計算しています…</p>;
  return (
    <>
      {busy ? (
        <output aria-live="polite" className="clef-bet-updating">
          更新中…
        </output>
      ) : null}
      <div aria-busy={busy} className={busy ? "clef-bet-loading" : undefined}>
        <BetCard hidden={hidden} state={state} />
      </div>
    </>
  );
}

function BetCard({ state, hidden }: { state: ClefBetState; hidden: readonly string[] }) {
  const card = state.card;
  if (card === null) return <p>オッズ発表後に推奨馬券を表示します。</p>;
  if (card === UNREACHABLE)
    return (
      <p>
        この予算では目標損益額に届く買い目がありません。予算を増やすか目標損益額を下げてください。
      </p>
    );
  if (card === REQUIRED_UNFIT)
    return (
      <p>
        この予算と目標損益額では、チェックした馬券の種類をすべて含める提案ができません。予算を増やすか、目標損益額を下げるか、「すべて含める」をオフにしてください。
      </p>
    );
  const runners = new Map(card.runners.map((runner) => [runner.horseNumber, runner]));
  const columns = PICK_COLUMNS.filter((column) => !hidden.includes(column.label));
  return (
    <>
      <p>{summarizePlanSet(card.plans)}</p>
      <div className="runner-table-wrap">
        <table className="clef-bet-table">
          <caption>オッズ取得: {card.oddsFetchedAt}</caption>
          <thead>
            <tr>
              <th scope="col">馬券</th>
              {columns.map((column) => (
                <th key={column.label} scope="col">
                  {column.label}
                </th>
              ))}
              <th scope="col">オッズ</th>
              <th scope="col">購入金額</th>
              <th scope="col">払戻額</th>
            </tr>
          </thead>
          <tbody>
            {card.plans.plans.map((plan) => (
              <tr key={`${plan.betType}-${plan.style}`}>
                <th scope="row">
                  <span className="clef-bet-type">{betTypeLabel(plan.betType)}</span>
                  <span className="clef-bet-style">{betStyleLabel(plan.style)}</span>
                  <span className="clef-bet-structure">{describePlan(plan)}</span>
                </th>
                {columns.map((column) =>
                  hidesFrame(column, plan) ? (
                    <td aria-label="枠番は組み合わせ馬券では表示しません" key={column.label} />
                  ) : (
                    <GroupCell
                      key={column.label}
                      pick={column.pick}
                      plan={plan}
                      runners={runners}
                    />
                  ),
                )}
                <td className="clef-bet-number">{formatPlanOdds(plan)}</td>
                <td className="clef-bet-number">{formatPlanStake(plan)}</td>
                <td className="clef-bet-number">{formatPlanPayout(plan)}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr>
              <th colSpan={columns.length + 3} scope="row">
                損益（払戻額 − 購入金額）
              </th>
              <td className="clef-bet-number">{formatPlanSetProfit(card.plans)}</td>
            </tr>
          </tfoot>
        </table>
      </div>
    </>
  );
}

// Budget entry: a labelled slider (arrow keys move 100 yen, PageUp/PageDown 1,000 yen), ±buttons
// and a numeric field that applies on blur or Enter so partial typing never snaps away.
function BudgetControl({ budget, onChange }: BudgetControlProps) {
  const id = useId();
  const [draft, setDraft] = useState<string>(String(budget));
  useEffect(() => setDraft(String(budget)), [budget]);
  const commit = (): void => {
    const next = normalizeBudget(Number(draft));
    setDraft(String(next));
    onChange(next);
  };
  const onSlide = (event: ChangeEvent<HTMLInputElement>): void =>
    onChange(normalizeBudget(Number(event.target.value)));
  const onKey = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === "Enter") commit();
  };
  return (
    <fieldset className="clef-bet-budget">
      <legend>予算</legend>
      <output aria-live="polite" className="clef-bet-budget-value" htmlFor={`${id}-range`}>
        {formatYen(budget)}
      </output>
      <div className="clef-bet-budget-controls">
        <button
          aria-label="予算を1,000円減らす"
          disabled={budget <= MIN_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget - LARGE_STEP))}
          type="button"
        >
          −1,000
        </button>
        <button
          aria-label="予算を100円減らす"
          disabled={budget <= MIN_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget - BET_BUDGET_STEP))}
          type="button"
        >
          −100
        </button>
        <input
          aria-label="予算スライダー"
          aria-valuetext={formatYen(budget)}
          id={`${id}-range`}
          max={SLIDER_MAX_BUDGET}
          min={MIN_BET_BUDGET}
          onChange={onSlide}
          step={BET_BUDGET_STEP}
          type="range"
          value={Math.min(budget, SLIDER_MAX_BUDGET)}
        />
        <button
          aria-label="予算を100円増やす"
          disabled={budget >= MAX_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget + BET_BUDGET_STEP))}
          type="button"
        >
          ＋100
        </button>
        <button
          aria-label="予算を1,000円増やす"
          disabled={budget >= MAX_BET_BUDGET}
          onClick={() => onChange(normalizeBudget(budget + LARGE_STEP))}
          type="button"
        >
          ＋1,000
        </button>
      </div>
      <div className="clef-bet-budget-input">
        <label htmlFor={`${id}-number`}>金額を入力（100円単位）</label>
        <input
          id={`${id}-number`}
          inputMode="numeric"
          max={MAX_BET_BUDGET}
          min={MIN_BET_BUDGET}
          onBlur={commit}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={onKey}
          step={BET_BUDGET_STEP}
          type="number"
          value={draft}
        />
        <span aria-hidden="true">円</span>
      </div>
    </fieldset>
  );
}

// 目標損益額: a numeric field that applies on blur or Enter like the typed budget.
function TargetControl({ target, onChange }: TargetControlProps) {
  const id = useId();
  const [draft, setDraft] = useState<string>(String(target));
  useEffect(() => setDraft(String(target)), [target]);
  const commit = (): void => {
    const next = normalizeTarget(Number(draft));
    setDraft(String(next));
    onChange(next);
  };
  const onKey = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === "Enter") commit();
  };
  return (
    <fieldset className="clef-bet-budget">
      <legend>目標損益額</legend>
      <div className="clef-bet-budget-input">
        <label htmlFor={id}>目標損益額を入力（円単位）</label>
        <input
          id={id}
          inputMode="numeric"
          max={MAX_BET_TARGET}
          min={MIN_BET_TARGET}
          onBlur={commit}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={onKey}
          step={1}
          type="number"
          value={draft}
        />
        <span aria-hidden="true">円</span>
      </div>
    </fieldset>
  );
}

// Shows suggested tickets under the realtime odds. The page polls realtime odds only while it is
// open, so suggestions follow each new snapshot only for viewers; the Worker re-runs clef-flash
// only when the favourites' order or the served prediction changes.
export function ClefBetSection(race: ClefRerankRaceAddress) {
  const [budget, setBudget] = useState<number>(DEFAULT_BET_BUDGET);
  const [target, setTarget] = useState<number>(DEFAULT_BET_TARGET);
  const [state, setState] = useState<ClefBetState | null>(null);
  const [hidden, setHidden] = useState<string[]>([]);
  const [types, setTypes] = useState<string[]>([...BET_TYPES]);
  const [requireAll, setRequireAll] = useState<boolean>(false);
  useEffect(() => {
    const mobile =
      typeof window.matchMedia === "function" && window.matchMedia(MOBILE_QUERY).matches;
    setHidden(readHiddenBetColumns(readStored(BET_COLUMNS_STORAGE_KEY), mobile));
    setTypes(readSelectedBetTypes(readStored(BET_TYPES_STORAGE_KEY)));
    setTarget(readStoredTarget(readStored(BET_TARGET_STORAGE_KEY)));
    setRequireAll(readStoredRequireAll(readStored(BET_REQUIRE_ALL_STORAGE_KEY)));
  }, []);
  const onRequireAll = (checked: boolean): void => {
    setRequireAll(checked);
    writeStored(BET_REQUIRE_ALL_STORAGE_KEY, checked);
  };
  const onTarget = (next: number): void => {
    setTarget(next);
    writeStored(BET_TARGET_STORAGE_KEY, next);
  };
  const onToggle = (label: string, visible: boolean): void => {
    const next = visible ? hidden.filter((item) => item !== label) : [...hidden, label];
    setHidden(next);
    writeStored(BET_COLUMNS_STORAGE_KEY, next);
  };
  const onToggleType = (type: string, checked: boolean): void => {
    const next = checked ? [...types, type] : types.filter((item) => item !== type);
    setTypes(next);
    writeStored(BET_TYPES_STORAGE_KEY, next);
  };
  const oddsFetchedAt = useRealtimeRaceSelector((store) => store.payload?.odds?.fetchedAt ?? "");
  const latest: Partial<Record<string, readonly unknown[]>> =
    useRealtimeRaceSelector((store) => store.payload?.odds?.latest) ?? {};
  const available = BET_TYPES.filter((type) => (latest[type]?.length ?? 0) > 0);
  const requested = requestedBetTypes(types, available);
  const url =
    available.length > 0 && requested.length === 0
      ? null
      : `${buildClefBetsUrl(race, budget, target, requested, requireAll)}&odds=${encodeURIComponent(oddsFetchedAt)}`;
  useEffect(() => {
    if (url === null) return undefined;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      const load = async (): Promise<void> => {
        const card = await loadCard(url, controller.signal);
        if (!controller.signal.aborted) setState({ url, card });
      };
      void load();
    }, FETCH_DELAY_MS);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [url]);
  return (
    <section aria-label="推奨馬券" className="clef-bet-section">
      <h3>推奨馬券</h3>
      <BudgetControl budget={budget} onChange={setBudget} />
      <TargetControl onChange={onTarget} target={target} />
      <BetTypeToggles
        available={available}
        onRequireAll={onRequireAll}
        onToggle={onToggleType}
        requireAll={requireAll}
        selected={types}
      />
      <ColumnToggles hidden={hidden} onToggle={onToggle} />
      {url === null ? (
        <p>オッズが発表されている馬券の種類を1つ以上選んでください。</p>
      ) : (
        <BetBody busy={state !== null && state.url !== url} hidden={hidden} state={state} />
      )}
    </section>
  );
}
