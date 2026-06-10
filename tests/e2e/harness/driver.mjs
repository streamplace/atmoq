// Drives writes against the PDS over plain XRPC, and records what the firehose
// should therefore contain. Output (stdout): a JSON summary consumed by verify.mjs.
//
// Operations exercised: account creation (=> #identity + #account), record
// creates, a record update, and a record delete (=> #commit ops).
const PDS_URL = process.env.PDS_URL ?? "http://localhost:2583";

async function xrpc(method, body, token) {
  const res = await fetch(`${PDS_URL}/xrpc/${method}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`${method} failed: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

const rkeyOf = (uri) => uri.split("/").pop();

const account = await xrpc("com.atproto.server.createAccount", {
  email: "alice@example.invalid",
  handle: "alice.test",
  password: "hunter2!!",
});
const { did, accessJwt } = account;

const expectedOps = [];

// two post creates
const posts = [];
for (const text of ["hello from atmoq e2e", "second post"]) {
  const post = await xrpc(
    "com.atproto.repo.createRecord",
    {
      repo: did,
      collection: "app.bsky.feed.post",
      record: {
        $type: "app.bsky.feed.post",
        text,
        createdAt: new Date().toISOString(),
      },
    },
    accessJwt,
  );
  posts.push(post);
  expectedOps.push({
    action: "create",
    path: `app.bsky.feed.post/${rkeyOf(post.uri)}`,
  });
}

// profile create, then update (putRecord twice on the same rkey)
for (const [i, displayName] of [["create", "Alice v1"], ["update", "Alice v2"]]) {
  await xrpc(
    "com.atproto.repo.putRecord",
    {
      repo: did,
      collection: "app.bsky.actor.profile",
      rkey: "self",
      record: { $type: "app.bsky.actor.profile", displayName },
    },
    accessJwt,
  );
  expectedOps.push({ action: i, path: "app.bsky.actor.profile/self" });
}

// delete the first post
await xrpc(
  "com.atproto.repo.deleteRecord",
  {
    repo: did,
    collection: "app.bsky.feed.post",
    rkey: rkeyOf(posts[0].uri),
  },
  accessJwt,
);
expectedOps.push({
  action: "delete",
  path: `app.bsky.feed.post/${rkeyOf(posts[0].uri)}`,
});

console.log(
  JSON.stringify({ did, handle: "alice.test", expectedOps }, null, 2),
);
