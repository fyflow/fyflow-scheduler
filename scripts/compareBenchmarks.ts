// Compare two benchmark JSON reports and print a markdown table.
//
// Absolute benchmark numbers from this suite are close to meaningless on their
// own: scenario order matters more than most code changes, and the same commit
// can measure 40-70% differently between runs. What IS meaningful is running two
// commits back to back on the same machine and comparing scenario by scenario,
// which is what .github/workflows/benchmark.yml does and what this reads.
//
// Run: deno run --allow-read scripts/compareBenchmarks.ts <base.json> <head.json>

export interface ComparisonRow {
  scenario: string;
  baseThroughput: number;
  headThroughput: number;
  throughputDelta: number;
  baseEfficiency: number;
  headEfficiency: number;
  /**
   * Percentage POINTS, not a relative percentage. Efficiency is already a
   * percentage, so a relative change of it amplifies noise near zero: two
   * values that both render as 0.0% once compared as -46.0%, which reads as a
   * catastrophe and means nothing.
   */
  efficiencyDeltaPoints: number;
}

interface SuiteLike {
  results?: Array<{
    config?: { name?: string };
    metrics?: { taskThroughput?: number; overallEfficiency?: number };
  }>;
}

/** Percentage change from a to b, 0 when the baseline is 0 or missing. */
export function percentDelta(from: number, to: number): number {
  if (!from) return 0;
  return ((to - from) / from) * 100;
}

/** Join two suites on scenario name, keeping only scenarios present in both. */
export function compareSuites(base: SuiteLike, head: SuiteLike): ComparisonRow[] {
  const byName = new Map<string, { throughput: number; efficiency: number }>();
  for (const r of base.results ?? []) {
    const name = r.config?.name;
    if (!name) continue;
    byName.set(name, {
      throughput: r.metrics?.taskThroughput ?? 0,
      efficiency: r.metrics?.overallEfficiency ?? 0
    });
  }

  const rows: ComparisonRow[] = [];
  for (const r of head.results ?? []) {
    const name = r.config?.name;
    if (!name) continue;
    const b = byName.get(name);
    // A scenario that ran on only one side cannot be compared; reporting it as a
    // huge delta against zero would be worse than leaving it out.
    if (!b) continue;
    const headThroughput = r.metrics?.taskThroughput ?? 0;
    const headEfficiency = r.metrics?.overallEfficiency ?? 0;
    rows.push({
      scenario: name,
      baseThroughput: b.throughput,
      headThroughput,
      throughputDelta: percentDelta(b.throughput, headThroughput),
      baseEfficiency: b.efficiency,
      headEfficiency,
      efficiencyDeltaPoints: headEfficiency - b.efficiency
    });
  }
  return rows;
}

/** Scenarios missing from one side, so a renamed or skipped scenario is visible. */
export function unmatched(base: SuiteLike, head: SuiteLike): { baseOnly: string[]; headOnly: string[] } {
  const names = (s: SuiteLike) =>
    new Set((s.results ?? []).map((r) => r.config?.name).filter((n): n is string => !!n));
  const b = names(base);
  const h = names(head);
  return {
    baseOnly: [...b].filter((n) => !h.has(n)),
    headOnly: [...h].filter((n) => !b.has(n))
  };
}

export function toMarkdown(
  rows: ComparisonRow[],
  meta: { baseRef: string; headRef: string; runs: string; missing: { baseOnly: string[]; headOnly: string[] } }
): string {
  const out: string[] = [];
  out.push(`### Benchmark: \`${meta.headRef}\` vs \`${meta.baseRef}\``);
  out.push("");
  out.push(
    `Both commits ran back to back on this runner, ${meta.runs} run(s) each. ` +
      `Compare the two columns against each other, not against any stored baseline.`
  );
  out.push("");
  out.push("| Scenario | Throughput base | Throughput head | Δ | Efficiency base | Efficiency head | Δ pp |");
  out.push("|---|---:|---:|---:|---:|---:|---:|");
  for (const r of rows) {
    const sign = (n: number) => `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
    const signPoints = (n: number) => `${n >= 0 ? "+" : ""}${n.toFixed(1)}pp`;
    out.push(
      `| ${r.scenario} | ${r.baseThroughput.toFixed(1)} | ${r.headThroughput.toFixed(1)} | ${sign(r.throughputDelta)} ` +
        `| ${r.baseEfficiency.toFixed(1)}% | ${r.headEfficiency.toFixed(1)}% | ${signPoints(r.efficiencyDeltaPoints)} |`
    );
  }
  if (!rows.length) out.push("| _no scenarios matched_ | | | | | | |");

  if (meta.missing.baseOnly.length || meta.missing.headOnly.length) {
    out.push("");
    out.push("**Not compared** (present on one side only):");
    for (const n of meta.missing.baseOnly) out.push(`- base only: ${n}`);
    for (const n of meta.missing.headOnly) out.push(`- head only: ${n}`);
  }

  out.push("");
  out.push(
    "> A delta here is not automatically a regression. This suite's run-to-run " +
      "spread on identical code has been measured above 100% on some scenarios. " +
      "Treat a single run as a prompt to investigate, not a verdict - re-run with " +
      "more runs, and check whether the changed code is even on the measured path."
  );
  return out.join("\n");
}

// CLI
if (import.meta.main) {
  const [basePath, headPath] = Deno.args.filter((a) => !a.startsWith("--"));
  if (!basePath || !headPath) {
    console.error("usage: compareBenchmarks.ts <base.json> <head.json>");
    Deno.exit(2);
  }
  const arg = (name: string, fallback: string) =>
    Deno.args.find((a) => a.startsWith(`--${name}=`))?.split("=")[1] ?? fallback;

  const base = JSON.parse(await Deno.readTextFile(basePath));
  const head = JSON.parse(await Deno.readTextFile(headPath));
  console.log(
    toMarkdown(compareSuites(base, head), {
      baseRef: arg("base-ref", "base"),
      headRef: arg("head-ref", "head"),
      runs: arg("runs", "1"),
      missing: unmatched(base, head)
    })
  );
}
