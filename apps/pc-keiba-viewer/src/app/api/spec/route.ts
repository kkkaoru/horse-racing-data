import { NextResponse } from "next/server";

export const dynamic = "force-static";

const jsonResponse = {
  content: {
    "application/json": {
      schema: {
        additionalProperties: true,
        type: "object",
      },
    },
  },
  description: "JSON response",
} as const;

const errorResponses = {
  "404": {
    content: {
      "application/json": {
        schema: {
          properties: {
            error: { type: "string" },
          },
          required: ["error"],
          type: "object",
        },
      },
    },
    description: "Not found",
  },
  "500": {
    content: {
      "application/json": {
        schema: {
          properties: {
            error: { type: "string" },
          },
          required: ["error"],
          type: "object",
        },
      },
    },
    description: "Server error",
  },
} as const;

const routeParameters = [
  {
    description: "開催年。例: 2026",
    in: "path",
    name: "year",
    required: true,
    schema: { pattern: "^\\d{4}$", type: "string" },
  },
  {
    description: "開催月。例: 05",
    in: "path",
    name: "month",
    required: true,
    schema: { pattern: "^\\d{2}$", type: "string" },
  },
  {
    description: "開催日。例: 18",
    in: "path",
    name: "day",
    required: true,
    schema: { pattern: "^\\d{2}$", type: "string" },
  },
  {
    description: "競馬場コード。例: 05, 35, 83",
    in: "path",
    name: "keibajoCode",
    required: true,
    schema: { pattern: "^[0-9A-Z]{2}$", type: "string" },
  },
  {
    description: "レース番号。例: 01, 12",
    in: "path",
    name: "raceNumber",
    required: true,
    schema: { pattern: "^\\d{2}$", type: "string" },
  },
] as const;

const sourceQueryParameter = {
  description: "レース情報の参照元。省略時はルート情報から判定されます。",
  in: "query",
  name: "source",
  required: false,
  schema: { enum: ["jra", "nar"], type: "string" },
} as const;

const raceSectionParameter = {
  description: "遅延読み込み対象の詳細セクション。",
  in: "path",
  name: "section",
  required: true,
  schema: {
    enum: [
      "ability",
      "bloodline",
      "condition",
      "finish-prediction",
      "overall-score",
      "pace-prediction",
      "results",
      "similar",
      "time-score",
      "training",
    ],
    type: "string",
  },
} as const;

const apiSpec = {
  info: {
    description:
      "PC-KEIBA Viewer がブラウザから利用する API の仕様です。レスポンススキーマは画面表示用の実データに追従するため、各レスポンスは追加プロパティを許可しています。",
    title: "PC-KEIBA Viewer Browser API",
    version: "1.0.0",
  },
  openapi: "3.1.0",
  paths: {
    "/api/spec": {
      get: {
        operationId: "getApiSpec",
        responses: {
          "200": jsonResponse,
        },
        summary: "この API 仕様を返します。",
        tags: ["spec"],
      },
    },
    "/api/models/gemma-4-e2b/{version}/gemma-4-E2B-it-web.task": {
      get: {
        operationId: "getGemma4E2BWebModel",
        parameters: [
          {
            description: "モデルバージョン。例: v20260518",
            in: "path",
            name: "version",
            required: true,
            schema: { pattern: "^v\\d{8}$", type: "string" },
          },
        ],
        responses: {
          "200": {
            content: {
              "application/octet-stream": {
                schema: { format: "binary", type: "string" },
              },
            },
            description: "MediaPipe LLM Inference 用 Gemma 4 E2B Web モデル。",
          },
          ...errorResponses,
        },
        summary: "ブラウザAI予想用の Gemma 4 E2B Web モデルを返します。",
        tags: ["models"],
      },
    },
    "/api/top-races": {
      get: {
        operationId: "getTopRaces",
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "トップページ用の注目レース一覧を返します。",
        tags: ["races"],
      },
    },
    "/api/cache-warm/race-detail-sections": {
      post: {
        operationId: "scheduleRaceDetailSectionCacheWarm",
        parameters: [
          {
            description: "デバッグ用。1 の場合のみ手動実行を許可します。",
            in: "query",
            name: "debug",
            required: false,
            schema: { enum: ["1"], type: "string" },
          },
          {
            description: "対象日。YYYY-MM-DD。省略時は Asia/Tokyo 基準の明日を対象にします。",
            in: "query",
            name: "date",
            required: false,
            schema: { pattern: "^\\d{4}-\\d{2}-\\d{2}$", type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "レース詳細のデフォルト遅延セクションキャッシュ作成をQueueへ投入します。",
        tags: ["cache"],
      },
    },
    "/api/mypage/favorites": {
      get: {
        operationId: "getFavoriteRaces",
        parameters: [
          {
            description:
              "お気に入り条件。horse, jockey, owner, trainer の各キーを複数指定できます。",
            in: "query",
            name: "horse",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "騎手のお気に入り ID。複数指定できます。",
            in: "query",
            name: "jockey",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "馬主のお気に入り ID。複数指定できます。",
            in: "query",
            name: "owner",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "調教師のお気に入り ID。複数指定できます。",
            in: "query",
            name: "trainer",
            required: false,
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "お気に入り条件に一致するレース一覧を返します。",
        tags: ["mypage"],
      },
    },
    "/api/mypage/favorites/search": {
      get: {
        operationId: "searchFavorites",
        parameters: [
          {
            description: "検索対象。",
            in: "query",
            name: "kind",
            required: true,
            schema: { enum: ["horse", "jockey", "owner", "trainer"], type: "string" },
          },
          {
            description: "検索文字列。",
            in: "query",
            name: "q",
            required: true,
            schema: { minLength: 1, type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "お気に入り追加用の候補を検索します。",
        tags: ["mypage"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/paddock": {
      get: {
        operationId: "getPaddockState",
        parameters: routeParameters,
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "パドック編集状態を返します。",
        tags: ["paddock"],
      },
      post: {
        operationId: "updatePaddockState",
        parameters: routeParameters,
        requestBody: {
          content: {
            "application/json": {
              schema: {
                additionalProperties: true,
                description: "パドック状態更新アクション。",
                type: "object",
              },
            },
          },
          required: true,
        },
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "パドック編集状態を更新します。",
        tags: ["paddock"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/paddock/live": {
      get: {
        operationId: "streamPaddockState",
        parameters: routeParameters,
        responses: {
          "200": {
            content: {
              "text/event-stream": {
                schema: { type: "string" },
              },
            },
            description: "Server-Sent Events stream",
          },
          ...errorResponses,
        },
        summary: "パドック状態のライブ更新 SSE を返します。",
        tags: ["paddock"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/paddock/discord": {
      post: {
        operationId: "sendPaddockDiscordNotification",
        parameters: routeParameters,
        requestBody: {
          content: {
            "application/json": {
              schema: {
                additionalProperties: true,
                description: "Discord 通知リクエスト。",
                type: "object",
              },
            },
          },
          required: true,
        },
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "パドック情報を Discord に通知します。",
        tags: ["paddock"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/premium": {
      get: {
        operationId: "getPremiumRaceData",
        parameters: [...routeParameters, sourceQueryParameter],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "JRA のプレミアム調教・厩舎コメント情報を返します。",
        tags: ["races"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/realtime": {
      get: {
        operationId: "getRealtimeRaceData",
        parameters: [...routeParameters, sourceQueryParameter],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "オッズ、馬体重、出走状態、着順などのリアルタイムデータを返します。",
        tags: ["races"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/ai/data": {
      get: {
        operationId: "getRaceAiDataParts",
        parameters: [
          ...routeParameters,
          sourceQueryParameter,
          {
            description:
              "AIが必要な実データだけを取得するためのカンマ区切り指定。例: race,runners,courseInfo,finishPrediction,overallScore",
            in: "query",
            name: "parts",
            required: false,
            schema: {
              default: "race,runners,courseInfo,courseDisplay",
              type: "string",
            },
          },
          {
            description:
              "parts に realtime を含めた時のリアルタイムデータの部分指定。例: entries,oddsTansho,weights,results,trackCondition",
            in: "query",
            name: "realtimeParts",
            required: false,
            schema: {
              default: "entries,oddsTansho,weights,results,trackCondition",
              type: "string",
            },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "WebGPU AI予想がオンデマンドで参照するレース実データをparts指定で返します。",
        tags: ["races"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/ai/logs": {
      delete: {
        operationId: "resetRaceAiLogsByDelete",
        parameters: [...routeParameters, sourceQueryParameter],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "レースごとのブラウザAI予想ログにサーバー側からリセットコマンドを送ります。",
        tags: ["races"],
      },
      get: {
        operationId: "getRaceAiLogServerCommand",
        parameters: [...routeParameters, sourceQueryParameter],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "レースごとのブラウザAI予想ログ向けサーバーコマンドを返します。",
        tags: ["races"],
      },
      post: {
        operationId: "setRaceAiLogServerCommand",
        parameters: [...routeParameters, sourceQueryParameter],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary:
          "レースごとのブラウザAI予想ログにresetコマンドを作成、またはackCommandIdで既読化します。",
        tags: ["races"],
      },
    },
    "/api/debug/ai-chat": {
      get: {
        operationId: "getLocalAiChatDebugSnapshot",
        parameters: [
          {
            description:
              "レースごとのAIチャット状態を参照するキー。省略時はlocalhost上の全スナップショットを返します。",
            in: "query",
            name: "raceKey",
            required: false,
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "localhost限定で、ブラウザAIチャットの状態を参照します。",
        tags: ["debug"],
      },
      post: {
        operationId: "sendLocalAiChatDebugCommand",
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary:
          "localhost限定で、ブラウザAIチャットへsend-message/reset/replace-messagesコマンドを渡します。",
        tags: ["debug"],
      },
      put: {
        operationId: "writeLocalAiChatDebugSnapshot",
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "localhost限定で、ブラウザAIチャットの状態を書き込みます。",
        tags: ["debug"],
      },
    },
    "/api/debug/ai-playground": {
      get: {
        operationId: "getAiPlaygroundDebugSnapshot",
        parameters: [
          {
            description:
              "/ai ページのブラウザ診断セッションID。省略時は最新セッションのサマリー一覧を返します。",
            in: "query",
            name: "sessionId",
            required: false,
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "/ai ページのブラウザログ、AI状態、ハートビート、サーバー同期状態を参照します。",
        tags: ["debug"],
      },
      post: {
        operationId: "clearAiPlaygroundDebugSnapshot",
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "/ai ページの指定セッション診断ログをサーバー側でリセットします。",
        tags: ["debug"],
      },
      put: {
        operationId: "writeAiPlaygroundDebugSnapshot",
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "/ai ページのブラウザログ、AI状態、ハートビートをサーバーへ書き込みます。",
        tags: ["debug"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/sections/{section}": {
      get: {
        operationId: "getRaceDetailSection",
        parameters: [
          ...routeParameters,
          raceSectionParameter,
          {
            description:
              "各セクションの表示条件。results, time-score, condition, bloodline, similar などで画面側の query をそのまま受け取ります。",
            in: "query",
            name: "*",
            required: false,
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "レース詳細画面の遅延読み込みセクションデータを返します。",
        tags: ["races"],
      },
    },
    "/api/races/{year}/{month}/{day}/{keibajoCode}/{raceNumber}/trends": {
      get: {
        operationId: "getRaceTrends",
        parameters: [
          ...routeParameters,
          sourceQueryParameter,
          {
            description: "騎手傾向の開始日。YYYY-MM-DD。",
            in: "query",
            name: "jockeyStart",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "騎手傾向の終了日。YYYY-MM-DD。",
            in: "query",
            name: "jockeyEnd",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "枠番傾向の開始日。YYYY-MM-DD。",
            in: "query",
            name: "frameStart",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "枠番傾向の終了日。YYYY-MM-DD。",
            in: "query",
            name: "frameEnd",
            required: false,
            schema: { type: "string" },
          },
          {
            description: "騎手傾向を同一競馬場に限定するか。",
            in: "query",
            name: "jockeySameVenue",
            required: false,
            schema: { default: true, type: "boolean" },
          },
          {
            description:
              "DBに未反映のリアルタイム結果を候補レースから補完するか。初期表示の高速化のため通常はfalse。",
            in: "query",
            name: "includeRealtimeResults",
            required: false,
            schema: { default: false, type: "boolean" },
          },
          {
            description: "脚質を集計条件から外すか。",
            in: "query",
            name: "runningStyleIgnoreRunningStyle",
            required: false,
            schema: { default: false, type: "boolean" },
          },
          {
            description: "枠番を脚質傾向の集計条件から外すか。",
            in: "query",
            name: "runningStyleIgnoreFrame",
            required: false,
            schema: { default: false, type: "boolean" },
          },
          {
            description: "騎手を脚質傾向の集計条件から外すか。",
            in: "query",
            name: "runningStyleIgnoreJockey",
            required: false,
            schema: { default: false, type: "boolean" },
          },
        ],
        responses: {
          "200": jsonResponse,
          ...errorResponses,
        },
        summary: "レース傾向の騎手別・枠番別集計を返します。",
        tags: ["races"],
      },
    },
  },
  servers: [
    {
      description: "Current origin",
      url: "/",
    },
  ],
  tags: [
    { name: "spec" },
    { name: "models" },
    { name: "races" },
    { name: "paddock" },
    { name: "mypage" },
    { name: "debug" },
  ],
} as const;

export function GET() {
  return NextResponse.json(apiSpec, {
    headers: {
      "Cache-Control": "public, max-age=300",
    },
  });
}
