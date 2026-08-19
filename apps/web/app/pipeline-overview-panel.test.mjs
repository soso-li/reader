import assert from "node:assert/strict";
import test from "node:test";

import { formatPipelineDateTime } from "./pipeline-overview-panel.tsx";


test("pipeline timestamps are stable between UTC server rendering and Shanghai browsers", () => {
  const previousTimezone = process.env.TZ;
  process.env.TZ = "UTC";
  try {
    assert.equal(
      formatPipelineDateTime("2026-07-18T05:30:00Z"),
      "2026/07/18 13:30"
    );
  } finally {
    if (previousTimezone === undefined) delete process.env.TZ;
    else process.env.TZ = previousTimezone;
  }
});
