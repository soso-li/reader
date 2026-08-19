import assert from "node:assert/strict";
import test from "node:test";

import { revealCurrentSettingsLink } from "./context-panel.tsx";

test("the active settings link is revealed inside a clipped mobile context panel", () => {
  let selector = "";
  let options;
  revealCurrentSettingsLink({
    querySelector(value) {
      selector = value;
      return { scrollIntoView(value) { options = value; } };
    }
  });

  assert.equal(selector, ".context-links [aria-current]");
  assert.deepEqual(options, { block: "nearest" });
});
