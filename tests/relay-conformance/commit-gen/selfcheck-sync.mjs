import { readFileSync } from "node:fs";
import { cborDecodeMulti, cborDecode } from "@atproto/common";
import { readCar, verifyCommitSig } from "@atproto/repo";
const cases = JSON.parse(readFileSync(new URL("../corpus-sync.json", import.meta.url)));
for (const c of cases) {
  const test = c.frames[c.frames.length-1];
  const setup = c.frames.find(f=>f.role==="setup");
  const bytes = Uint8Array.from(Buffer.from(test.hex,"hex"));
  try {
    const [header,payload] = cborDecodeMulti(bytes);
    let info = `t=${header.t}`;
    if (header.t === "#sync") {
      const {roots,blocks} = await readCar(payload.blocks);
      const sig = await verifyCommitSig(cborDecode(blocks.get(payload.commit ?? roots[0])), c.signingKey);
      info += ` rev=${payload.rev} sig=${sig} blocks=${[...blocks.cids()].length}`;
    } else {
      const {roots,blocks} = await readCar(payload.blocks);
      const commit = cborDecode(blocks.get(payload.commit));
      const sig = await verifyCommitSig(commit, c.signingKey);
      // setup rev for comparison
      let setupRev = "-";
      if (setup) { const [,sp]=cborDecodeMulti(Uint8Array.from(Buffer.from(setup.hex,"hex"))); setupRev=sp.rev; }
      const prevData = payload.prevData ? payload.prevData.toString().slice(-8) : "<none>";
      const commitData = commit.data.toString().slice(-8);
      info += ` setupRev=${setupRev} testRev=${payload.rev} sig=${sig} prevData=${prevData} rebase=${payload.rebase}`;
    }
    console.log(c.id.padEnd(32), info);
  } catch(e){ console.log(c.id.padEnd(32), "ERR:", e.message.slice(0,60)); }
}
