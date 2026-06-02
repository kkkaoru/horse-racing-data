// Run with: bunx vitest run src/app/mypage/user-identity-panel.test.tsx

import "fake-indexeddb/auto";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { setUserId } from "../../lib/user-identity-indexeddb";
import { UserIdentityPanel } from "./user-identity-panel";

const DB_NAME = "pc-keiba-viewer";

const resetIndexedDb = (): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.addEventListener("success", () => {
      resolve();
    });
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("blocked", () => {
      reject(new Error("indexedDB deleteDatabase blocked"));
    });
  });

beforeEach(async () => {
  await resetIndexedDb();
  vi.restoreAllMocks();
});

afterEach(() => {
  cleanup();
});

test("displays-current-user-id", async () => {
  await setUserId("preset-display-id");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("preset-display-id")).toBeTruthy();
  });
});

test("saves-new-user-id-on-button-click", async () => {
  await setUserId("initial-id");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("initial-id")).toBeTruthy();
  });
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "next_user-99" } });
  fireEvent.click(screen.getByRole("button", { name: "保存" }));
  await waitFor(() => {
    expect(screen.getByText("next_user-99")).toBeTruthy();
  });
});

test("validation-rejects-invalid-chars", async () => {
  await setUserId("ok-id");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("ok-id")).toBeTruthy();
  });
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "invalid id with spaces" } });
  fireEvent.click(screen.getByRole("button", { name: "保存" }));
  await waitFor(() => {
    expect(screen.getByText("英数字、 ハイフン、 アンダースコアのみ使用できます。")).toBeTruthy();
  });
  expect(screen.getByText("ok-id")).toBeTruthy();
});

test("validation-rejects-empty-value", async () => {
  await setUserId("orig");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("orig")).toBeTruthy();
  });
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "   " } });
  fireEvent.click(screen.getByRole("button", { name: "保存" }));
  await waitFor(() => {
    expect(screen.getByText("1文字以上で入力してください。")).toBeTruthy();
  });
});

test("regenerate-button-creates-new-id", async () => {
  await setUserId("before-regenerate");
  const randomSpy = vi.spyOn(crypto, "randomUUID");
  randomSpy.mockReturnValue("33333333-3333-4333-8333-333333333333");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("before-regenerate")).toBeTruthy();
  });
  fireEvent.click(screen.getByRole("button", { name: "再生成" }));
  await waitFor(() => {
    expect(screen.getByText("33333333-3333-4333-8333-333333333333")).toBeTruthy();
  });
});

test("shows-saved-status-after-save", async () => {
  await setUserId("start");
  render(<UserIdentityPanel />);
  await waitFor(() => {
    expect(screen.getByText("start")).toBeTruthy();
  });
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "saved-now" } });
  fireEvent.click(screen.getByRole("button", { name: "保存" }));
  await waitFor(() => {
    expect(screen.getByText("保存済み")).toBeTruthy();
  });
});

test("shows-server-note-about-local-storage", async () => {
  render(<UserIdentityPanel />);
  expect(
    screen.getByText(
      "IDは localStorage や IndexedDB のような自分のブラウザ内のみに保存され、 サーバには送信されません (paddock 評価更新時を除く)。",
    ),
  ).toBeTruthy();
});
