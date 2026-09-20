// This file runs with Bun. Database targets require an explicit command argument.
import { runHistoryCli, type HistoryCliReport, type HistoryCliRuntime } from "./history-cli";
import {
  runHistoryPublicationOperator,
  type HistoryPublicationReport,
} from "./history-publication-operator";
import {
  createHistoryConnection,
  type HistoryConnection,
  type HistoryDatabaseTarget,
} from "./storage/history-connection";

export interface HistoryCommandInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly runtime: HistoryCliRuntime;
}
interface PublicationArguments {
  readonly target: HistoryDatabaseTarget;
  readonly argv: readonly string[];
}

const publicationArguments = (args: readonly string[]): PublicationArguments => {
  const [action, directory, target, confirmation]: readonly (string | undefined)[] = args;
  const validAction: boolean =
    (action === "prepare" && args.length === 3) ||
    (action === "apply" && args.length === 4 && confirmation === "--confirm-write");
  if (!validAction || !directory || (target !== "local" && target !== "production")) {
    throw new Error(
      "Use prepare DIRECTORY TARGET or apply DIRECTORY TARGET --confirm-write; TARGET is local or production.",
    );
  }
  return {
    target,
    argv: action === "prepare" ? ["prepare", directory] : ["apply", directory, "--confirm-write"],
  };
};

export const runHistoryCommand = async (
  input: HistoryCommandInput,
): Promise<HistoryCliReport | HistoryPublicationReport> => {
  if (input.argv[0] === "collect" || input.argv[0] === "status") return runHistoryCli(input);
  const args: PublicationArguments = publicationArguments(input.argv);
  const connection: HistoryConnection = createHistoryConnection(input.env, args.target);
  try {
    return await runHistoryPublicationOperator({
      argv: args.argv,
      targetFingerprint: connection.targetFingerprint,
      database: connection.database,
      files: input.runtime,
    });
  } finally {
    await connection.close();
  }
};
