/**
 * @file example.ts
 * @description End-to-end example demonstrating all major features of the
 * experimental-database client. Run against a live server:
 *
 *   cargo run -- data 127.0.0.1:3000
 *   npx ts-node example.ts
 */

import { DbClient } from "./client";

async function example(): Promise<void> {
  const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
  console.log("Connected.");

  // ── Collections ─────────────────────────────────────────────────────────

  await db.createCollection("products");
  console.log("Created collection 'products'.");

  // ── Insert ───────────────────────────────────────────────────────────────

  const tx = db.mutation();
  const appleId  = await tx.insert("products", { name: "Apple",  price: 1,  active: true  });
  const bananaId = await tx.insert("products", { name: "Banana", price: 3,  active: true  });
  const cherryId = await tx.insert("products", { name: "Cherry", price: 10, active: false });
  const dateId   = await tx.insert("products", { name: "Date",   price: 15, active: true  });
  const outcome  = await tx.commit();
  console.log("Inserted 4 docs, outcome:", outcome);

  // ── Patch ─────────────────────────────────────────────────────────────────

  const patch = db.mutation();
  await patch.patch("products", bananaId, { price: 4, organic: true });
  await patch.commit();
  console.log(`Patched banana (${bananaId}): price → 4, added organic=true.`);

  // ── Read back ─────────────────────────────────────────────────────────────

  const q1 = db.query();
  const banana = await q1.get("products", bananaId);
  await q1.commit();
  console.log("Banana after patch:", banana);

  // ── Filters ───────────────────────────────────────────────────────────────

  const q2 = db.query();

  // price > 2
  const expensive = await q2.find("products", { gt: ["price", 2] });
  console.log("price > 2:", expensive.map((d) => d["name"]));

  // active=true AND price >= 3
  const activeAndCheap = await q2.find("products", {
    and: [{ eq: ["active", true] }, { gte: ["price", 3] }],
  });
  console.log("active=true AND price >= 3:", activeAndCheap.map((d) => d["name"]));

  // price IN [1, 10]  — returns only IDs (cheaper)
  const inPriceIds = await q2.findIds("products", { in: ["price", [1, 10]] });
  console.log("price IN [1,10] ids:", inPriceIds);

  await q2.commit();

  // ── Subscription ──────────────────────────────────────────────────────────

  const qSub = db.query({ subscribe: true });
  await qSub.find("products", null);          // subscribe to the full collection
  const subOutcome = await qSub.commit();

  if (subOutcome.subscribed) {
    const sub = subOutcome.subscription;
    console.log(`Subscribed with id=${sub.id}. Waiting for an invalidation…`);

    const invalidated = new Promise<void>((resolve) => {
      sub.onInvalidate((event) => {
        console.log(`Invalidation received at ts=${event.ts} for sub=${event.sub_id}.`);
        sub.cancel();
        resolve();
      });
    });

    // Trigger the invalidation by writing from a second connection
    const db2 = await DbClient.connect("ws://127.0.0.1:3000/ws");
    const tx2 = db2.mutation();
    await tx2.insert("products", { name: "Elderberry", price: 7, active: true });
    await tx2.commit();
    db2.close();

    await invalidated;
  }

  // ── OCC conflict detection ────────────────────────────────────────────────

  const db3 = await DbClient.connect("ws://127.0.0.1:3000/ws");

  const m1 = db.mutation();
  const m2 = db3.mutation();

  // Both transactions read the same doc
  await m1.get("products", appleId);
  await m2.get("products", appleId);

  // Both try to patch it
  await m1.patch("products", appleId, { price: 2 });
  await m2.patch("products", appleId, { price: 99 });

  const r1 = await m1.commit();
  const r2 = await m2.commit();

  console.log("OCC m1 result:", r1.result);   // "committed"
  console.log("OCC m2 result:", r2.result);   // "conflict"
  db3.close();

  // ── Delete ────────────────────────────────────────────────────────────────

  const del = db.mutation();
  await del.delete("products", cherryId);
  await del.delete("products", dateId);
  await del.commit();
  console.log(`Deleted cherry (${cherryId}) and date (${dateId}).`);

  const q3 = db.query();
  const remaining = await q3.find("products", null);
  await q3.commit();
  console.log("Remaining products:", remaining.map((d) => d["name"]));

  // ── Cleanup ───────────────────────────────────────────────────────────────

  await db.deleteCollection("products");
  console.log("Deleted collection 'products'.");

  db.close();
  console.log("Done.");
}

example().catch((e) => {
  console.error(e);
  process.exit(1);
});
