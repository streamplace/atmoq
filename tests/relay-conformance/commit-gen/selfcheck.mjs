import { readFileSync } from "node:fs";
import { cborDecodeMulti, cborDecode } from "@atproto/common";
import { readCar, verifyCommitSig } from "@atproto/repo";
const cases = JSON.parse(readFileSync(new URL("../corpus-commit.json", import.meta.url)));
for (const c of cases) {
  const bytes = Uint8Array.from(Buffer.from(c.hex, "hex"));
  try {
    const [header, payload] = cborDecodeMulti(bytes);
    let carNote, sig="n/a";
    try {
      const { roots, blocks } = await readCar(payload.blocks);
      const commitObj = cborDecode(blocks.get(payload.commit));
      sig = await verifyCommitSig(commitObj, c.signingKey);
      carNote = `rootOk=${roots[0].equals(payload.commit)}`;
    } catch(e){ carNote = "CAR-reject:"+e.message.slice(0,42); }
    console.log(c.id.padEnd(26), `sig=${sig}`, carNote);
  } catch (e) { console.log(c.id.padEnd(26), "FRAME ERR:", e.message); }
}
