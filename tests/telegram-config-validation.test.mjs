import test from "node:test";
import assert from "node:assert/strict";
import { TelegramBridge } from "../packages/bridge-telegram/dist/src/index.js";

test("TelegramBridge validates required config", () => {
  assert.throws(
    () =>
      new TelegramBridge({
        botToken: "",
        defaultChatId: "",
      }),
    /invalid-telegram-bridge-config/,
  );
});
