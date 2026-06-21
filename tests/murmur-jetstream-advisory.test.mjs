import assert from "node:assert/strict";
import test from "node:test";
import { startJetStreamAdvisoryDlqIfEnabled } from "../scripts/murmur-jetstream-advisory.mjs";

test("startJetStreamAdvisoryDlqIfEnabled is dormant when JetStream is disabled", async () => {
  let calls = 0;
  const result = await startJetStreamAdvisoryDlqIfEnabled({
    jetstreamEnabled: false,
    outbox: {},
    broker: {
      async startJetStreamAdvisoryDlq() {
        calls += 1;
      },
    },
  });

  assert.equal(result, undefined);
  assert.equal(calls, 0);
});

test("startJetStreamAdvisoryDlqIfEnabled wires advisory DLQ when JetStream is enabled", async () => {
  const outbox = {};
  const subscription = { unsubscribe() {} };
  const logs = [];
  const calls = [];

  const result = await startJetStreamAdvisoryDlqIfEnabled({
    jetstreamEnabled: true,
    outbox,
    log: (level, msg) => logs.push({ level, msg }),
    broker: {
      async startJetStreamAdvisoryDlq(params) {
        calls.push(params);
        return subscription;
      },
    },
  });

  assert.equal(result, subscription);
  assert.deepEqual(calls, [{ outbox }]);
  assert.deepEqual(logs, [{ level: "info", msg: "JetStream advisory DLQ correlation started" }]);
});
