// Boots the local atproto network (PLC + PDS) on fixed ports and stays alive.
// Used as the first process inside the e2e container; see ../entrypoint.sh.
import { TestNetwork } from "../dev-env/dist/index.js";

const PLC_PORT = 2582;
const PDS_PORT = 2583;

const network = await TestNetwork.create({
  plc: { port: PLC_PORT },
  pds: { port: PDS_PORT },
});

console.log(
  JSON.stringify({
    msg: "dev-env network ready",
    plc: network.plc.url,
    pds: network.pds.url,
  }),
);

const shutdown = async () => {
  await network.close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// stay alive until signalled
await new Promise(() => {});
