import { assertEquals } from "jsr:@std/assert@1";
import { compareSuites, percentDelta, toMarkdown, unmatched } from "./compareBenchmarks.ts";

const suite = (rows: Array<[string, number, number]>) => ({
  results: rows.map(([name, taskThroughput, overallEfficiency]) => ({
    config: { name },
    metrics: { taskThroughput, overallEfficiency }
  }))
});

Deno.test("percentDelta is signed and relative to the base", () => {
  assertEquals(percentDelta(100, 150), 50);
  assertEquals(percentDelta(100, 50), -50);
  assertEquals(percentDelta(100, 100), 0);
});

Deno.test("a zero baseline does not produce Infinity", () => {
  // Dividing by a missing baseline would render as Infinity% and read as a
  // catastrophic regression rather than as missing data.
  assertEquals(percentDelta(0, 500), 0);
});

Deno.test("scenarios are joined by name, not by position", () => {
  // Categories do not always run in the same order, so index-joining would
  // silently compare two different scenarios against each other.
  const base = suite([["alpha", 100, 90], ["beta", 200, 80]]);
  const head = suite([["beta", 220, 84], ["alpha", 90, 81]]);
  const rows = compareSuites(base, head);
  assertEquals(rows.map((r) => r.scenario), ["beta", "alpha"]);
  assertEquals(rows[0].throughputDelta, 10);
  assertEquals(rows[1].throughputDelta, -10);
});

Deno.test("a scenario present on only one side is excluded, not compared to zero", () => {
  const rows = compareSuites(suite([["alpha", 100, 90]]), suite([["alpha", 110, 90], ["gamma", 50, 10]]));
  assertEquals(rows.length, 1);
  assertEquals(rows[0].scenario, "alpha");
});

Deno.test("unmatched scenarios are reported from both sides", () => {
  const m = unmatched(suite([["alpha", 1, 1], ["dropped", 1, 1]]), suite([["alpha", 1, 1], ["added", 1, 1]]));
  assertEquals(m.baseOnly, ["dropped"]);
  assertEquals(m.headOnly, ["added"]);
});

Deno.test("efficiency is reported in percentage points, not relative percent", () => {
  // Efficiency is already a percentage. A relative delta of it explodes near
  // zero - 0.04% vs 0.02% is a -50% "regression" that is pure rounding.
  const rows = compareSuites(suite([["alpha", 100, 0.04]]), suite([["alpha", 100, 0.02]]));
  assertEquals(Number(rows[0].efficiencyDeltaPoints.toFixed(2)), -0.02);
  const md = toMarkdown(rows, { baseRef: "a", headRef: "b", runs: "1", missing: { baseOnly: [], headOnly: [] } });
  assertEquals(md.includes("pp"), true);
  assertEquals(md.includes("-50.0%"), false);
});

Deno.test("markdown carries the sign and the caveat", () => {
  const md = toMarkdown(compareSuites(suite([["alpha", 100, 50]]), suite([["alpha", 120, 55]])), {
    baseRef: "v1", headRef: "v2", runs: "3", missing: { baseOnly: [], headOnly: [] }
  });
  assertEquals(md.includes("+20.0%"), true);
  assertEquals(md.includes("`v2` vs `v1`"), true);
  // The caveat is the point of the report - a bare table invites false alarms
  assertEquals(md.includes("not automatically a regression"), true);
});

Deno.test("an empty comparison renders a table rather than crashing", () => {
  const md = toMarkdown([], { baseRef: "a", headRef: "b", runs: "1", missing: { baseOnly: [], headOnly: [] } });
  assertEquals(md.includes("no scenarios matched"), true);
});
