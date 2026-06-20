import test from "node:test";
import assert from "node:assert/strict";
import { buildNatsConnectionOptions } from "../packages/broker-nats/dist/src/index.js";

test("buildNatsConnectionOptions enables resilient reconnect defaults", () => {
  const options = buildNatsConnectionOptions({
    url: "nats://example.invalid:4222",
    token: "secret",
  });

  assert.equal(options.servers, "nats://example.invalid:4222");
  assert.equal(options.token, "secret");
  assert.equal(options.maxReconnectAttempts, -1);
  assert.equal(options.reconnectTimeWait, 2000);
  assert.equal(options.reconnectJitter, 500);
  assert.equal(options.pingInterval, 20000);
  assert.equal(options.maxPingOut, 2);
  assert.equal(options.waitOnFirstConnect, true);
});

test("buildNatsConnectionOptions allows bounded operator overrides", () => {
  const options = buildNatsConnectionOptions({
    url: "nats://example.invalid:4222",
    maxReconnectAttempts: 10,
    reconnectTimeWait: 5000,
    reconnectJitter: 1000,
    pingInterval: 30000,
    maxPingOut: 3,
    waitOnFirstConnect: false,
  });

  assert.equal(options.maxReconnectAttempts, 10);
  assert.equal(options.reconnectTimeWait, 5000);
  assert.equal(options.reconnectJitter, 1000);
  assert.equal(options.pingInterval, 30000);
  assert.equal(options.maxPingOut, 3);
  assert.equal(options.waitOnFirstConnect, false);
});
