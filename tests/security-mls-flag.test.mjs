import test from "node:test";
import assert from "node:assert/strict";
import { isMlsEnabled } from "../packages/security/dist/src/index.js";

test("isMlsEnabled reflects env flag", () => {
  const prev = process.env.MURMUR_ENABLE_MLS;
  process.env.MURMUR_ENABLE_MLS = "1";
  assert.equal(isMlsEnabled(), true);

  process.env.MURMUR_ENABLE_MLS = "0";
  assert.equal(isMlsEnabled(), false);

  if (prev === undefined) delete process.env.MURMUR_ENABLE_MLS;
  else process.env.MURMUR_ENABLE_MLS = prev;
});
