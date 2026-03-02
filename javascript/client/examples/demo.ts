/**
 * @file demo.ts
 * End-to-end demonstration of all major features of experimental-db-client.
 *
 * Run against a live server:
 *   cargo run -- data 127.0.0.1:3000
 *   bun run examples/demo.ts
 */

import { DbClient } from "../index.ts";

async function demo(): Promise<void> {
  const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
  console.log("Connected.\n");

  // ── Collections ─────────────────────────────────────────────────────────

  await db.createCollection("products");
  console.log("Created collection 'products'.");

  // ── Insert ───────────────────────────────────────────────────────────────

  const tx = db.mutation();
  const appleId  = await tx.insert("products", { name: "Apple",  price: 1,  active: true  });
  const bananaId = await tx.insert("products", { name: "Banana", price: 3,  active: true  });
  const cherryId = await tx.insert("products", { name: "Cherry", price: 10, active: false });
  const dateId   = await tx.insert("products", { name: "Date",   price: 15, active: true  });
  const inserted = await tx.commit();
  console.log(`Inserted 4 docs →`, inserted);

  // ── Patch ─────────────────────────────────────────────────────────────────

  const patch = db.mutation();
  await patch.patch("products", bananaId, { price: 4, organic: true });
  await patch.commit();
  console.log(`Patched banana: price → 4, added organic=true.`);

  // ── Point read ────────────────────────────────────────────────────────────

  const q1 = db.query();
  const banana = await q1.get("products", bananaId);
  await q1.commit();
  console.log("Banana after patch:", banana);

  // ── Filter queries ────────────────────────────────────────────────────────

  console.log("\n── Filters ──");
  const q2 = db.query();

  const expensive = await q2.find("products", { gt: ["price", 2] });
  console.log("price > 2         :", expensive.map((d) => d["name"]));

  const activeAndCheap = await q2.find("products", {
    and: [{ eq: ["active", true] }, { gte: ["price", 3] }],
  });
  console.log("active ∧ price ≥ 3:", activeAndCheap.map((d) => d["name"]));

  const inPriceIds = await q2.findIds("products", { in: ["price", [1, 10]] });
  console.log("price ∈ {1, 10}   :", inPriceIds);

  await q2.commit();

  // ── Live subscription ─────────────────────────────────────────────────────

  console.log("\n── Subscription ──");
  const qSub = db.query({ subscribe: true });
  await qSub.find("products", null);
  const subOutcome = await qSub.commit();

  if (subOutcome.subscribed) {
    const sub = subOutcome.subscription;
    console.log(`Subscribed (id=${sub.id}). Triggering a write from a second connection…`);

    const invalidated = new Promise<void>((resolve) => {
      sub.onInvalidate((event) => {
        console.log(`Invalidation received at ts=${event.ts}.`);
        sub.cancel();
        resolve();
      });
    });

    const db2 = await DbClient.connect("ws://127.0.0.1:3000/ws");
    const tx2 = db2.mutation();
    await tx2.insert("products", { name: "Elderberry", price: 7, active: true });
    await tx2.commit();
    db2.close();

    await invalidated;
  }

  // ── OCC conflict detection ────────────────────────────────────────────────

  console.log("\n── OCC conflict ──");
  const db3 = await DbClient.connect("ws://127.0.0.1:3000/ws");

  const m1 = db.mutation();
  const m2 = db3.mutation();

  // Both read the same document
  await m1.get("products", appleId);
  await m2.get("products", appleId);

  // Both try to write to it
  await m1.patch("products", appleId, { price: 2 });
  await m2.patch("products", appleId, { price: 99 });

  const r1 = await m1.commit();
  const r2 = await m2.commit();
  console.log("m1 result:", r1.result);  // "committed"
  console.log("m2 result:", r2.result);  // "conflict"
  db3.close();

  // ── Delete ────────────────────────────────────────────────────────────────

  console.log("\n── Delete ──");
  const del = db.mutation();
  await del.delete("products", cherryId);
  await del.delete("products", dateId);
  await del.commit();

  const q3 = db.query();
  const remaining = await q3.find("products", null);
  await q3.commit();
  console.log("Remaining:", remaining.map((d) => d["name"]));

  // ── Cleanup ───────────────────────────────────────────────────────────────

  await db.deleteCollection("products");
  db.close();
  console.log("\nDone.");
}

demo().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
