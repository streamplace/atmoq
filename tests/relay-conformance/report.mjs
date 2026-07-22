// Build the HTML conformance report from the live results/*.jsonl files.
//
//   node report.mjs            # writes relay-conformance.html next to this file
//
// One self-contained, theme-aware HTML file (no external assets) — safe to host
// anywhere. Regenerate after re-running any relay harness.
import fs from "node:fs";

const base = new URL(".", import.meta.url).pathname;
const RELAYS = ["atmoq", "indigo", "rsky", "hydrant", "zlay"];
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const clean = (s) => String(s).replace(/[^\x20-\x7e]/g, "?");

const loadCorpus = (f) => JSON.parse(fs.readFileSync(base + f));
const loadResults = (suffix) => Object.fromEntries(RELAYS.map((r) => {
  const p = `${base}results/${r}${suffix}.jsonl`;
  if (!fs.existsSync(p)) return [r, null];
  const m = {};
  fs.readFileSync(p, "utf8").split("\n").filter(Boolean).forEach((l) => { const o = JSON.parse(l); m[o.id] = o; });
  return [r, m];
}));

const CONTROL = new Set(["drisl/float64-ok-payload", "drisl/tag-42-ok-payload", "commit/valid", "sync11/sync-event-valid", "sync11/commit2-valid"]);
const FWD = new Set(["sync/unknown-type", "sync/unknown-field", "commit/record-unknown-type"]);
const REPORTED = new Set(["commit/record-no-type"]);
const DAGGER = new Set([
  "drisl/float16-payload", "drisl/float32-payload", "drisl/nan-payload",
  "drisl/infinity-payload", "drisl/undefined-payload", "drisl/simple-value-payload",
  "drisl/tag-0-payload", "drisl/tag-2-bignum-payload", "drisl/tag-42-no-prefix-payload",
]);

const chip = (o) => {
  if (!o) return `<span class="chip pending" title="harness result pending">&middot;&middot;&middot;</span>`;
  const cls = o.outcome === "reject" ? "reject" : o.outcome === "skip" ? "skip" : "accept";
  const label = o.outcome === "reject" ? "REJECT" : o.outcome === "skip" ? "SKIP" : "ACCEPT";
  return `<span class="chip ${cls}" title="${esc(clean(o.detail || ""))}">${label}</span>`;
};

function section(corpusFile, suffix, title, subtitle, layerDefs) {
  const corpus = loadCorpus(corpusFile);
  const R = loadResults(suffix);
  let uRej = 0, uAcc = 0, dis = 0, ready = RELAYS.every((r) => R[r]);

  const consensusOf = (id) => {
    const v = RELAYS.map((r) => R[r]?.[id]?.outcome).filter(Boolean);
    if (v.length < RELAYS.length) return "pending";
    const s = new Set(v);
    if (s.size > 1) return "split";
    return v[0] === "reject" ? "reject" : "accept";
  };

  const rowsFor = (layer) => corpus.filter((c) => c.layer === layer).map((c) => {
    const con = consensusOf(c.id);
    if (con === "split") dis++; else if (con === "reject") uRej++; else if (con === "accept") uAcc++;
    const shortId = c.id.split("/")[1];
    const tags = [];
    if (CONTROL.has(c.id)) tags.push(`<span class="tag control">control &middot; must accept</span>`);
    if (FWD.has(c.id)) tags.push(`<span class="tag fwd">forward-compat</span>`);
    if (REPORTED.has(c.id)) tags.push(`<span class="tag rep">reported case</span>`);
    if (DAGGER.has(c.id)) tags.push(`<span class="tag dag" title="Defect carried in an unknown field; struct decoders skip it without inspecting its encoding.">&dagger;&nbsp;in unknown field</span>`);
    const cells = RELAYS.map((r) => `<td class="v">${chip(R[r]?.[c.id])}</td>`).join("");
    return `<tr class="row ${con}">
      <td class="case"><div class="case-title">${esc(c.title)}</div>
        <div class="case-id">${esc(shortId)}${tags.length ? " " + tags.join(" ") : ""}</div></td>
      ${cells}
      <td class="consensus"><span class="dot ${con}" title="${con}"></span></td></tr>`;
  }).join("\n");

  const bodies = layerDefs.map(([key, name, desc]) => {
    const rows = rowsFor(key);
    if (!rows) return "";
    return `<tbody class="layer">
      <tr class="layer-head"><th colspan="${RELAYS.length + 2}"><span class="lname">${name}</span><span class="ldesc">${desc}</span></th></tr>
      ${rows}</tbody>`;
  }).join("\n");

  const total = corpus.length;
  const stat = ready
    ? `<span class="mini"><b style="color:var(--flag)">${dis}</b> disagree</span><span class="mini"><b>${uRej}</b> all reject</span><span class="mini"><b>${uAcc}</b> all accept</span><span class="mini">${total} cases</span>`
    : `<span class="mini">${total} cases &middot; <span style="color:var(--flag)">results pending for ${RELAYS.filter((r) => !R[r]).join(", ")}</span></span>`;

  return `<div class="sec-head"><div><h2 id="${suffix || "account"}">${title}</h2><p class="sec-sub">${subtitle}</p></div><div class="ministat">${stat}</div></div>
  <div class="tablecard"><div class="scroll"><table>
    <thead><tr class="head"><th>Malformed case</th>
      ${RELAYS.map((r) => `<th class="v">${r}</th>`).join("")}<th class="consensus"></th></tr></thead>
    ${bodies}
  </table></div></div>`;
}

const ACCOUNT_LAYERS = [
  ["framing", "Frame framing", "the two-object wire contract"],
  ["cbor", "CBOR well-formedness", "can the bytes be decoded at all?"],
  ["drisl", "DRISL determinism", "valid CBOR, non-canonical encoding"],
  ["drisl-float", "Floats & simple values", "float width, NaN, undefined"],
  ["drisl-tag", "Tags & keys", "tag 42 only, text keys, UTF-8"],
  ["at-sync", "at-sync semantics", "valid encoding, event shape"],
];
const COMMIT_LAYERS = [
  ["commit", "Valid baseline", "correct CAR, CIDs, signature"],
  ["commit-envelope", "Envelope encoding", "DRISL on the outer payload map"],
  ["commit-record", "Record contents", "$type & record shape — inside the CAR"],
  ["commit-repo", "Repo & crypto integrity", "CAR CIDs, blocks, signature, flags"],
];
const SYNC_LAYERS = [
  ["sync-event", "#sync event", "sync-1.1's compact repo-state message"],
  ["sync-commit2", "Stateful commit checks", "prevData, rev order, rebase — need prior state"],
];

const POLICY = {
  atmoq: { tag: "Rust · DRISL-strict", reject: "drops the <b>frame</b>, connection stays up",
    body: "Validates the whole frame against DRISL at ingest — <b>encoding only</b>. It treats the commit's CAR as opaque bytes, so it does no signature, CID, MST, or record checks (that's its unbuilt M2)." },
  indigo: { tag: "Go · cbor-gen · production", reject: "drops the <b>whole PDS connection</b> &amp; reconnects",
    body: "A frame CBOR-decode error unwinds the socket. Commit repo/MST/signature failures instead drop one event — and MST/record-presence checks are <b>advisory</b> (logged, not enforced)." },
  rsky: { tag: "Rust · serde_ipld_dagcbor", reject: "drops the <b>event</b>, connection stays up",
    body: "Lenient ciborium header, stricter dag-cbor body. CAR CID integrity &amp; envelope checks are enforced; but default <b>lenient mode publishes signature failures</b>, and record-presence is a TODO." },
  hydrant: { tag: "Rust · jacquard · strictest default", reject: "CBOR error drops the <b>connection</b>; else drops the <b>event</b>",
    body: "Always resolves the signing key, so signature verify <b>and</b> MST inversion run by default. Uniquely decodes record CBOR and <b>enforces prevData correctness</b> (inverts the MST and checks the root). Unknown types skip one frame, not the socket. <span class=\"mono\">verify_cids</span> off by default." },
  zlay: { tag: "Zig · zat SDK · fail-open", reject: "drops the <b>frame</b>, connection stays up",
    body: "<span class=\"mono\">zat.cbor</span> is a strict whole-frame DAG-CBOR decoder, so it matches atmoq on encoding — and goes further, rejecting <b>all</b> floats incl. float64. But its commit/sync path is <b>fail-open</b>: a bad signature, bad CID, or cache miss <em>forwards the frame unvalidated</em> and re-resolves the key in the background — it never drops a commit on crypto." },
};
const policyCards = RELAYS.map((r) => `<article class="policy ${r}">
  <header><span class="pname">${r}</span><span class="ptag">${POLICY[r].tag}</span></header>
  <p class="preject"><span class="k">on reject</span> ${POLICY[r].reject}</p>
  <p class="pbody">${POLICY[r].body}</p></article>`).join("\n");

const accountSection = section("corpus.json", "", "Malformed #account frames", "Signature- and CAR-free events: a reject can only mean the encoding or shape was rejected.", ACCOUNT_LAYERS);
const commitSection = section("corpus-commit.json", "-commit", "Malformed #commit frames", "Real signed commits carrying a CAR of blocks — the validation that matters happens below frame-decode.", COMMIT_LAYERS);
const syncSection = section("corpus-sync.json", "-sync", "sync-1.1 compliance", "The at-synchronization rules: the #sync event, and the prevData / rev-ordering checks that only fire on a second commit once the relay holds prior repo state.", SYNC_LAYERS);

const html = `<meta charset="utf-8">
<title>atproto relay conformance — invalid firehose data</title>
<style>
:root{
  --ground:#f4f6f9; --panel:#ffffff; --panel-2:#f9fafc;
  --ink:#151820; --muted:#5c6472; --faint:#8a92a1; --hairline:#e3e7ee;
  --accent:#4451d6; --accent-soft:#eceeff;
  --reject-bg:#e8eafc; --reject-ink:#3340c0; --reject-br:#c9cef6;
  --accept-bg:#f7ecd6; --accept-ink:#8a5a17; --accept-br:#ecd9b4;
  --flag:#d9902a; --flag-soft:#fbf3e6;
  --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:ui-monospace,"SF Mono","Cascadia Code","JetBrains Mono",Menlo,Consolas,monospace;
}
@media (prefers-color-scheme:dark){:root{
  --ground:#0e1015; --panel:#161922; --panel-2:#1b1f2a;
  --ink:#e7eaf1; --muted:#9aa2b2; --faint:#6b7484; --hairline:#262b37;
  --accent:#8b95f0; --accent-soft:#1c2140;
  --reject-bg:#22284f; --reject-ink:#b8c0ff; --reject-br:#343d72;
  --accept-bg:#3a2f1a; --accept-ink:#e9c88a; --accept-br:#54431f;
  --flag:#e0a955; --flag-soft:#2b2415;
}}
:root[data-theme="light"]{
  --ground:#f4f6f9; --panel:#ffffff; --panel-2:#f9fafc;
  --ink:#151820; --muted:#5c6472; --faint:#8a92a1; --hairline:#e3e7ee;
  --accent:#4451d6; --accent-soft:#eceeff;
  --reject-bg:#e8eafc; --reject-ink:#3340c0; --reject-br:#c9cef6;
  --accept-bg:#f7ecd6; --accept-ink:#8a5a17; --accept-br:#ecd9b4;
  --flag:#d9902a; --flag-soft:#fbf3e6;
}
:root[data-theme="dark"]{
  --ground:#0e1015; --panel:#161922; --panel-2:#1b1f2a;
  --ink:#e7eaf1; --muted:#9aa2b2; --faint:#6b7484; --hairline:#262b37;
  --accent:#8b95f0; --accent-soft:#1c2140;
  --reject-bg:#22284f; --reject-ink:#b8c0ff; --reject-br:#343d72;
  --accept-bg:#3a2f1a; --accept-ink:#e9c88a; --accept-br:#54431f;
  --flag:#e0a955; --flag-soft:#2b2415;
}
*{box-sizing:border-box}
body{margin:0;background:var(--ground);color:var(--ink);font-family:var(--sans);line-height:1.55;-webkit-font-smoothing:antialiased;font-size:16px}
.wrap{max-width:1060px;margin:0 auto;padding:clamp(20px,5vw,56px) clamp(16px,4vw,40px) 64px}
a{color:var(--accent)}
.mono{font-family:var(--mono);font-size:.92em}
.eyebrow{font-family:var(--mono);font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--accent);font-weight:600}
h1{font-size:clamp(28px,4.6vw,44px);line-height:1.08;letter-spacing:-.02em;margin:.35em 0 .1em;text-wrap:balance;font-weight:700}
.sub{font-size:clamp(16px,2vw,19px);color:var(--muted);max-width:66ch;margin:.4em 0 0}
.meta{font-family:var(--mono);font-size:12.5px;color:var(--faint);margin-top:14px;display:flex;gap:18px;flex-wrap:wrap}
.meta b{color:var(--muted);font-weight:600}
.thesis{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--hairline);border:1px solid var(--hairline);border-radius:14px;overflow:hidden;margin:34px 0 8px}
.stat{background:var(--panel);padding:20px 22px}
.stat .n{font-family:var(--mono);font-size:34px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums;line-height:1}
.stat.big .n{color:var(--flag)}
.stat .l{font-size:13.5px;color:var(--muted);margin-top:8px;max-width:28ch}
.stat .frac{font-size:15px;color:var(--faint);font-family:var(--mono)}
.thesis-note{font-size:12.5px;color:var(--faint);margin:10px 2px 0}
h2{font-size:20px;letter-spacing:-.015em;font-weight:700;margin:0}
.sec-head{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap;margin:46px 0 14px;padding-bottom:12px;border-bottom:1px solid var(--hairline)}
.sec-sub{margin:5px 0 0;font-size:13.5px;color:var(--muted);max-width:60ch}
.ministat{display:flex;gap:14px;font-family:var(--mono);font-size:12px;color:var(--muted);white-space:nowrap}
.ministat b{font-size:15px;color:var(--ink)}
.mini{display:flex;gap:5px;align-items:baseline}
.h2bar{font-size:13px;font-family:var(--mono);letter-spacing:.12em;text-transform:uppercase;color:var(--muted);font-weight:600;margin:44px 0 16px;padding-bottom:10px;border-bottom:1px solid var(--hairline)}
.policies{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}
.policy{background:var(--panel);border:1px solid var(--hairline);border-radius:12px;padding:16px 17px;border-top:3px solid var(--accent)}
.policy header{display:flex;align-items:baseline;justify-content:space-between;gap:8px;flex-wrap:wrap}
.pname{font-family:var(--mono);font-size:17px;font-weight:700}
.ptag{font-family:var(--mono);font-size:10.5px;color:var(--faint);letter-spacing:.02em}
.preject{margin:12px 0 8px;font-size:14px}
.preject .k{display:block;font-family:var(--mono);font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--faint);margin-bottom:2px}
.pbody{margin:0;font-size:13px;color:var(--muted);line-height:1.5}
/* overflow:clip (not hidden/auto) keeps the rounded corners + wide-table clipping
   WITHOUT establishing a scroll container, so the sticky header below references
   the page viewport and floats as you scroll. Horizontal scroll returns on narrow
   screens via the media query at the end. */
.tablecard{background:var(--panel);border:1px solid var(--hairline);border-radius:14px;overflow:clip;margin-top:4px}
.scroll{overflow-x:clip}
table{border-collapse:collapse;width:100%;min-width:800px}
thead th{position:sticky;top:0;background:var(--panel);z-index:5}
.head th{text-align:left;padding:16px 14px 12px;font-family:var(--mono);font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);font-weight:600;box-shadow:inset 0 -1px 0 var(--hairline),0 6px 10px -8px rgba(0,0,0,.22)}
.head th.v{text-align:center;font-size:14px;letter-spacing:0;text-transform:none;color:var(--ink);font-weight:700}
.head th.consensus{width:34px}
.layer-head th{padding:14px 14px 8px;background:var(--panel-2);border-bottom:1px solid var(--hairline)}
.lname{font-weight:700;font-size:14px;margin-right:12px}
.ldesc{color:var(--faint);font-size:12.5px;font-family:var(--mono)}
.row td{padding:11px 14px;border-bottom:1px solid var(--hairline);vertical-align:middle}
tbody.layer:last-child .row:last-child td{border-bottom:none}
.row.split{background:linear-gradient(90deg,var(--flag-soft),transparent 60%)}
.row.split td.case{box-shadow:inset 3px 0 0 var(--flag)}
.case-title{font-size:14.5px;font-weight:600;letter-spacing:-.005em}
.case-id{font-family:var(--mono);font-size:11.5px;color:var(--faint);margin-top:3px;display:flex;gap:7px;flex-wrap:wrap;align-items:center}
td.v{text-align:center;white-space:nowrap}
.chip{display:inline-block;font-family:var(--mono);font-size:11px;font-weight:600;letter-spacing:.05em;padding:4px 9px;border-radius:6px;border:1px solid transparent}
.chip.reject{background:var(--reject-bg);color:var(--reject-ink);border-color:var(--reject-br)}
.chip.accept{background:var(--accept-bg);color:var(--accept-ink);border-color:var(--accept-br)}
.chip.skip{background:transparent;color:var(--faint);border-color:var(--hairline)}
.chip.pending{background:transparent;color:var(--faint);border-color:var(--hairline);letter-spacing:.15em}
.consensus{text-align:center}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%}
.dot.split{background:var(--flag)}
.dot.reject{background:var(--reject-br)}
.dot.accept{background:var(--accept-br)}
.dot.pending{background:var(--hairline)}
.tag{font-family:var(--mono);font-size:9.5px;font-weight:600;letter-spacing:.04em;padding:1.5px 6px;border-radius:4px;text-transform:uppercase}
.tag.control,.tag.fwd{background:var(--accent-soft);color:var(--accent)}
.tag.rep{background:var(--flag-soft);color:var(--flag)}
.tag.dag{background:transparent;color:var(--faint);border:1px solid var(--hairline);text-transform:none;letter-spacing:0}
.legend{display:flex;gap:20px;flex-wrap:wrap;font-size:12.5px;color:var(--muted);font-family:var(--mono);margin:14px 2px 0;align-items:center}
.legend .li{display:flex;align-items:center;gap:7px}
.findings{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:4px}
.finding{background:var(--panel);border:1px solid var(--hairline);border-radius:12px;padding:16px 18px}
.finding h3{margin:0 0 6px;font-size:15px;font-weight:700;letter-spacing:-.01em}
.finding p{margin:0;font-size:13.5px;color:var(--muted);line-height:1.55}
.finding .mono{font-family:var(--mono);font-size:12.5px;color:var(--ink)}
.foot{margin-top:44px;padding-top:18px;border-top:1px solid var(--hairline);font-family:var(--mono);font-size:12px;color:var(--faint);line-height:1.7}
.foot b{color:var(--muted);font-weight:600}
@media (max-width:820px){.thesis,.policies,.findings{grid-template-columns:1fr}}
/* below the table's min-width, re-enable horizontal scroll (sticky header yields here) */
@media (max-width:900px){.scroll{overflow-x:auto}}
@media (prefers-reduced-motion:reduce){*{scroll-behavior:auto}}
</style>

<div class="wrap">
  <header>
    <div class="eyebrow">atproto relay conformance · invalid firehose data</div>
    <h1>Five relays, five definitions of &ldquo;invalid&rdquo;</h1>
    <p class="sub">The same malformed <span class="mono">subscribeRepos</span> frame, fed to each relay's real decoder and commit-verifier — Rust, Go, and Zig. One defect per frame, so every verdict is attributable — across <span class="mono">#account</span> events, real signed <span class="mono">#commit</span>s carrying a CAR of blocks, and stateful <span class="mono">#sync</span>/sync-1.1 commit sequences.</p>
    <div class="meta"><span><b>49</b> cases</span><span><b>5</b> relays</span><span><b>#account</b> · <b>#commit</b> · <b>sync-1.1</b></span><span>2026-07-21</span></div>
  </header>

  <section class="thesis">
    <div class="stat big"><div class="n">22<span class="frac">&thinsp;/&thinsp;32</span></div><div class="l"><span class="mono">#account</span> cases where at least two relays <b style="color:var(--flag)">disagree</b></div></div>
    <div class="stat"><div class="n">3<span class="frac">&thinsp;/&thinsp;5</span></div><div class="l">relays that <b>forward</b> a commit signed by the <b>wrong key</b> (atmoq, rsky, zlay) — only indigo &amp; hydrant drop it</div></div>
    <div class="stat"><div class="n">1<span class="frac">&thinsp;/&thinsp;5</span></div><div class="l">relays that enforce <span class="mono">prevData</span> correctness (<b>hydrant</b>) — sync-1.1's core guarantee, honored by one</div></div>
  </section>
  <p class="thesis-note">Even the <span class="mono">float64</span> &ldquo;control&rdquo; splits: zlay's DAG-CBOR decoder rejects <em>all</em> floats, so only the tag-42 control and the valid signed commit are accepted by all five.</p>

  <div class="h2bar">What a &ldquo;reject&rdquo; actually does</div>
  <div class="policies">${policyCards}</div>

  ${accountSection}
  <div class="legend">
    <span class="li"><span class="chip reject">REJECT</span> event/frame dropped</span>
    <span class="li"><span class="chip accept">ACCEPT</span> passed through</span>
    <span class="li"><span class="dot split"></span> relays disagree</span>
    <span class="li">&dagger; defect sits in an unknown field the struct decoders skip</span>
    <span class="li">hover any verdict for the exact reason</span>
  </div>

  ${commitSection}
  <div class="legend">
    <span class="li">verdict = the <b style="color:var(--ink)">relay-level</b> decision (drop or publish), mirroring enforce-vs-advisory gating &mdash; not just whether a verify function errored</span>
  </div>

  ${syncSection}
  <div class="legend">
    <span class="li">stateful: a valid <b style="color:var(--ink)">setup</b> commit runs first, then the frame under test &mdash; so <span class="mono">prevData</span> / rev checks actually fire</span>
    <span class="li">rsky verdicts are its <b style="color:var(--ink)">default lenient</b> mode; strict mode is stricter (see tooltip)</span>
    <span class="li">zlay is <b style="color:var(--ink)">fail-open</b>: an ACCEPT on a bad sig / prevData means forwarded <em>unvalidated</em>, not verified</span>
  </div>

  <div class="h2bar">What to look at</div>
  <div class="findings">
    <div class="finding"><h3>Fail-open vs fail-closed: the same bad commit, opposite fates</h3>
      <p>Give five relays a commit signed by the <em>wrong key</em>: <span class="mono">hydrant</span> and indigo <b>drop</b> it; <span class="mono">zlay</span> <b>forwards it unvalidated</b> (and re-resolves the key in the background), rsky publishes it with a warning, atmoq has no crypto. zlay is fail-open by design — it never drops a commit on bad signature, CID, or MST. hydrant is its mirror image. The spec never says which is conformant.</p></div>
    <div class="finding"><h3>sync-1.1's <span class="mono">prevData</span> guarantee is enforced by exactly one relay</h3>
      <p>A commit whose <span class="mono">prevData</span> is <em>present but wrong</em> (the MST inversion fails) is dropped only by <span class="mono">hydrant</span> — it always holds the signing key, so it always inverts the MST. The other four accept it. The property that lets a consumer verify an op <em>without</em> fetching the repo is honored by one relay of five. MUST, or hint?</p></div>
    <div class="finding"><h3>Even the <span class="mono">float64</span> control isn't universal</h3>
      <p>The float64 case is a <em>control</em> — valid DRISL, meant to be accepted everywhere. It is, by four relays. <span class="mono">zlay</span>'s <span class="mono">zat</span> DAG-CBOR decoder rejects <b>all</b> floats, float64 included — atproto records carry no floats, so it forbids them outright. Two whole-frame validators (atmoq, zlay) enforce DRISL; they still disagree on whether float64 is even legal.</p></div>
    <div class="finding"><h3>No relay validates record <em>contents</em></h3>
      <p>A <span class="mono">#commit</span> whose record omits <span class="mono">$type</span> — or is a list, not a map — passes all five. Only hydrant decodes record bodies at all (as generic CBOR), so it alone would reject a <em>malformed-CBOR</em> record; none require a lexicon <span class="mono">$type</span>. Record semantics are left to PDSes and AppViews.</p></div>
    <div class="finding"><h3>The blast radius differs, not just the verdict</h3>
      <p>A bad frame costs atmoq/rsky/zlay one event; it costs <span class="mono">indigo</span> and <span class="mono">hydrant</span> the whole connection to that PDS until reconnect — a mild DoS foothold. And rsky is still the one relay whose parser <em>errors</em> on an unknown message type (<span class="mono">#futurething</span>); the other four tolerate it. Specify the <em>consequence</em>, not only reject-or-not.</p></div>
    <div class="finding"><h3>Strictness is a per-relay default, not a spec</h3>
      <p>Two relays validate the whole frame's encoding (atmoq, zlay); three decode into structs and never re-check it. rsky's default publishes bad sigs and rev rollbacks; hydrant enforces them; zlay forwards them. indigo drops deprecated <span class="mono">tooBig</span>/<span class="mono">rebase</span> flags that others ignore. Five codebases, five postures — a spec's MUST list should say which is conformant, and match what ships <em>enabled by default</em>.</p></div>
  </div>

  <div class="foot">
    <div><b>Method.</b> Each verdict runs the relay's real code: atmoq <span class="mono">Frame::parse</span>; indigo cbor-gen decode + <span class="mono">VerifyRepoCommit</span> gating; rsky <span class="mono">SubscribeReposEvent</span> parse + commit validation; hydrant <span class="mono">decode_frame</span> + <span class="mono">validate_commit</span>/<span class="mono">validate_sync</span> at its default posture; zlay <span class="mono">zat.cbor</span> decode + <span class="mono">Validator.validateCommit</span>/<span class="mono">validateSync</span> with the signing key pre-seeded (fail-open forward on any failure). Commits are genuine signed repos built with @atproto/repo. Corpus &amp; harness: <b>tests/relay-conformance/</b> in the atmoq repo.</div>
    <div style="margin-top:8px"><b>Fidelity notes.</b> atmoq does no repo/crypto validation yet (its M2), so it accepts any commit with a valid envelope. The reject-<em>consequence</em> row (connection vs event) is from ingest source, not a live capture; an end-to-end injection harness would measure it directly.</div>
  </div>
</div>`;

const out = base + "relay-conformance.html";
fs.writeFileSync(out, html);
console.log("wrote", html.length, "bytes ->", out);
