/**
 * @file demo_ecommerce.ts
 * @description Large e-commerce dataset demo. Populates collections for
 * categories, products, customers, orders, and reviews.
 *
 *   cargo run -- data 127.0.0.1:3000
 *   bun run examples/demo_ecommerce.ts
 */

import { DbClient } from "../index.ts";

// ── Helpers ──────────────────────────────────────────────────────────────────

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min: number, max: number, decimals = 2): number {
  return parseFloat((Math.random() * (max - min) + min).toFixed(decimals));
}

function randBool(trueProbability = 0.5): boolean {
  return Math.random() < trueProbability;
}

function randDate(start: Date, end: Date): string {
  return new Date(
    start.getTime() + Math.random() * (end.getTime() - start.getTime())
  ).toISOString();
}

async function batchInsert(
  db: DbClient,
  collection: string,
  docs: Record<string, unknown>[],
  batchSize = 50
): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < docs.length; i += batchSize) {
    const batch = docs.slice(i, i + batchSize);
    const tx = db.mutation();
    for (const doc of batch) {
      ids.push(await tx.insert(collection, doc));
    }
    await tx.commit();
  }
  return ids;
}

// ── Raw data fixtures ─────────────────────────────────────────────────────────

const CATEGORY_TREE = [
  { name: "Electronics",   slug: "electronics",   icon: "💻", featured: true },
  { name: "Clothing",      slug: "clothing",       icon: "👕", featured: true },
  { name: "Home & Garden", slug: "home-garden",    icon: "🏡", featured: true },
  { name: "Sports",        slug: "sports",         icon: "⚽", featured: false },
  { name: "Books",         slug: "books",          icon: "📚", featured: true },
  { name: "Toys",          slug: "toys",           icon: "🧸", featured: false },
  { name: "Beauty",        slug: "beauty",         icon: "💄", featured: true },
  { name: "Automotive",    slug: "automotive",     icon: "🚗", featured: false },
  { name: "Food",          slug: "food",           icon: "🍕", featured: true },
  { name: "Music",         slug: "music",          icon: "🎵", featured: false },
];

const BRANDS = [
  "TechPro", "NovaBrand", "EcoLife", "UrbanEdge", "PrimePick",
  "BlueStar", "GreenLeaf", "SpeedRun", "CloudNine", "DarkHorse",
  "BrightSide", "IronForge", "SilkTouch", "FrostByte", "SunValley",
];

const FIRST_NAMES = [
  "Alice","Bob","Carol","Dave","Eva","Frank","Grace","Hank","Iris","Jake",
  "Karen","Leo","Mia","Noah","Olivia","Paul","Quinn","Rachel","Sam","Tina",
  "Uma","Victor","Wendy","Xander","Yara","Zach","Amy","Brian","Chloe","Dylan",
  "Emma","Felix","Gina","Hugo","Ivy","Josh","Kira","Liam","Maya","Ned",
  "Opal","Pete","Ruby","Scott","Tara","Ursula","Vance","Willa","Xerxes","Yvonne",
];

const LAST_NAMES = [
  "Smith","Jones","Brown","Wilson","Taylor","Davies","Evans","Thomas","Roberts",
  "Johnson","Williams","Walker","White","Martin","Clarke","Thompson","Baker",
  "Hall","Turner","Harris","Lewis","Robinson","Jackson","Wood","Allen","King",
  "Adams","Green","Young","Hill","Scott","Carter","Phillips","Campbell","Parker",
];

const CITIES = [
  { city: "New York",    state: "NY", country: "US", zip: "10001" },
  { city: "Los Angeles", state: "CA", country: "US", zip: "90001" },
  { city: "Chicago",     state: "IL", country: "US", zip: "60601" },
  { city: "Houston",     state: "TX", country: "US", zip: "77001" },
  { city: "Phoenix",     state: "AZ", country: "US", zip: "85001" },
  { city: "London",      state: "",   country: "GB", zip: "EC1A" },
  { city: "Toronto",     state: "ON", country: "CA", zip: "M5H" },
  { city: "Sydney",      state: "NSW",country: "AU", zip: "2000" },
  { city: "Berlin",      state: "",   country: "DE", zip: "10115" },
  { city: "Paris",       state: "",   country: "FR", zip: "75001" },
];

const ORDER_STATUSES = ["pending","processing","shipped","delivered","cancelled","refunded"];
const PAYMENT_METHODS = ["credit_card","debit_card","paypal","apple_pay","google_pay","bank_transfer"];
const CARRIERS = ["FedEx","UPS","DHL","USPS","Amazon Logistics"];
const REVIEW_ADJECTIVES = [
  "Amazing","Terrible","Decent","Outstanding","Mediocre","Fantastic","Awful",
  "Solid","Disappointing","Superb","Overpriced","Worth every penny","So-so",
  "Exceeded expectations","Not as described",
];

// ── Product catalog fixtures ───────────────────────────────────────────────────

const PRODUCTS_BY_CATEGORY: Record<string, Array<{
  name: string; basePrice: number; tags: string[];
}>> = {
  electronics: [
    { name: "Wireless Earbuds Pro",      basePrice: 79,   tags: ["audio","wireless","portable"] },
    { name: "4K Smart TV 55\"",           basePrice: 649,  tags: ["tv","4k","smart"] },
    { name: "Mechanical Keyboard RGB",   basePrice: 119,  tags: ["keyboard","gaming","rgb"] },
    { name: "USB-C Hub 7-Port",          basePrice: 39,   tags: ["hub","usb","accessory"] },
    { name: "Noise-Cancelling Headphones",basePrice: 299, tags: ["audio","premium","noise-cancel"] },
    { name: "Portable SSD 1TB",          basePrice: 89,   tags: ["storage","portable","fast"] },
    { name: "Webcam 4K 60fps",           basePrice: 149,  tags: ["webcam","streaming","4k"] },
    { name: "Laptop Stand Aluminium",    basePrice: 45,   tags: ["laptop","ergonomic","stand"] },
    { name: "Smart Watch Series X",      basePrice: 349,  tags: ["wearable","health","smart"] },
    { name: "Wireless Charging Pad 15W", basePrice: 29,   tags: ["charging","wireless","fast"] },
    { name: "Gaming Mouse 16000 DPI",    basePrice: 69,   tags: ["mouse","gaming","precision"] },
    { name: "Portable Projector 1080p",  basePrice: 399,  tags: ["projector","portable","home"] },
    { name: "Bluetooth Speaker IP67",    basePrice: 59,   tags: ["audio","bluetooth","waterproof"] },
    { name: "Dash Cam 4K Front+Rear",    basePrice: 149,  tags: ["camera","car","safety"] },
    { name: "E-Reader 8\" Glare-Free",   basePrice: 139,  tags: ["ereader","books","portable"] },
  ],
  clothing: [
    { name: "Classic White T-Shirt",     basePrice: 19,   tags: ["casual","cotton","unisex"] },
    { name: "Slim Fit Chinos",           basePrice: 49,   tags: ["pants","formal","slim"] },
    { name: "Puffer Jacket Midnight",    basePrice: 119,  tags: ["jacket","winter","warm"] },
    { name: "Running Shorts 7\"",        basePrice: 34,   tags: ["running","sport","breathable"] },
    { name: "Merino Wool Sweater",       basePrice: 89,   tags: ["sweater","premium","wool"] },
    { name: "Leather Belt Brown",        basePrice: 29,   tags: ["belt","leather","accessory"] },
    { name: "Summer Floral Dress",       basePrice: 55,   tags: ["dress","summer","floral"] },
    { name: "High-Waist Yoga Leggings",  basePrice: 44,   tags: ["yoga","sport","stretch"] },
    { name: "Linen Button-Up Shirt",     basePrice: 39,   tags: ["shirt","linen","casual"] },
    { name: "Waterproof Hiking Boots",   basePrice: 129,  tags: ["boots","hiking","outdoor"] },
    { name: "Reversible Windbreaker",    basePrice: 79,   tags: ["jacket","rain","outdoor"] },
    { name: "Cashmere Beanie",           basePrice: 35,   tags: ["hat","cashmere","winter"] },
  ],
  "home-garden": [
    { name: "Robot Vacuum Cleaner",      basePrice: 299,  tags: ["robot","vacuum","smart"] },
    { name: "Air Purifier HEPA H13",     basePrice: 179,  tags: ["air","purifier","health"] },
    { name: "Cast Iron Skillet 12\"",    basePrice: 49,   tags: ["kitchen","cast-iron","cooking"] },
    { name: "Bamboo Cutting Board Set",  basePrice: 34,   tags: ["kitchen","bamboo","eco"] },
    { name: "LED Desk Lamp Dimmable",    basePrice: 39,   tags: ["lamp","led","dimmable"] },
    { name: "Smart Thermostat WiFi",     basePrice: 149,  tags: ["smart","thermostat","energy"] },
    { name: "Duvet Insert King Size",    basePrice: 89,   tags: ["bedding","king","warm"] },
    { name: "Succulent Planter Set 3pc", basePrice: 24,   tags: ["plants","decor","ceramic"] },
    { name: "French Press Coffee Maker", basePrice: 29,   tags: ["coffee","kitchen","glass"] },
    { name: "Electric Kettle 1.7L",      basePrice: 44,   tags: ["kitchen","kettle","fast"] },
    { name: "Shower Head Rainfall",      basePrice: 59,   tags: ["bathroom","shower","luxury"] },
    { name: "Scented Soy Candle Set",    basePrice: 32,   tags: ["candle","scent","gift"] },
  ],
  sports: [
    { name: "Adjustable Dumbbells 32kg", basePrice: 199,  tags: ["weights","home-gym","adjustable"] },
    { name: "Yoga Mat 6mm Non-slip",     basePrice: 29,   tags: ["yoga","mat","exercise"] },
    { name: "Resistance Bands Set 5pc",  basePrice: 19,   tags: ["bands","exercise","portable"] },
    { name: "Pull-Up Bar Doorframe",     basePrice: 34,   tags: ["bodyweight","door","gym"] },
    { name: "Cycling Helmet MIPS",       basePrice: 79,   tags: ["cycling","safety","helmet"] },
    { name: "Running Shoes V12",         basePrice: 129,  tags: ["running","shoes","cushion"] },
    { name: "Tennis Racket Pro Series",  basePrice: 89,   tags: ["tennis","racket","sport"] },
    { name: "Jump Rope Speed Cable",     basePrice: 14,   tags: ["jump-rope","cardio","speed"] },
    { name: "Foam Roller 18\"",          basePrice: 24,   tags: ["recovery","foam","muscle"] },
    { name: "Basketball Official Size",  basePrice: 39,   tags: ["basketball","outdoor","sport"] },
  ],
  books: [
    { name: "The Art of Clean Code",     basePrice: 29,   tags: ["programming","tech","clean-code"] },
    { name: "Atomic Habits",             basePrice: 18,   tags: ["habits","self-help","popular"] },
    { name: "Sapiens: A Brief History",  basePrice: 16,   tags: ["history","science","popular"] },
    { name: "Deep Work",                 basePrice: 17,   tags: ["productivity","focus","work"] },
    { name: "Designing Data-Intensive Applications", basePrice: 55, tags: ["database","programming","systems"] },
    { name: "The Pragmatic Programmer",  basePrice: 45,   tags: ["programming","career","tech"] },
    { name: "System Design Interview",   basePrice: 36,   tags: ["systems","interview","tech"] },
    { name: "Zero to One",               basePrice: 16,   tags: ["startup","business","innovation"] },
  ],
};

// ── Main demo ─────────────────────────────────────────────────────────────────

async function demo(): Promise<void> {
  const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
  console.log("Connected to database.");

  // ── 1. Create collections ───────────────────────────────────────────────────

  const collections = ["categories","products","customers","orders","order_items","reviews","coupons"];
  for (const col of collections) {
    await db.createCollection(col);
  }
  console.log(`Created ${collections.length} collections.`);

  // ── 2. Categories ───────────────────────────────────────────────────────────

  const now = new Date();
  const yearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);

  const categoryIds = await batchInsert(db, "categories",
    CATEGORY_TREE.map((c, i) => ({
      ...c,
      sort_order: i,
      product_count: 0,
      created_at: randDate(yearAgo, now),
      seo_title: `Buy ${c.name} Online | Best Deals`,
      seo_description: `Shop the best ${c.name.toLowerCase()} at unbeatable prices.`,
    }))
  );
  console.log(`Inserted ${categoryIds.length} categories.`);

  // ── 3. Products (one per fixture, each with variants) ──────────────────────

  const allProducts: Record<string, unknown>[] = [];
  for (const [slug, fixtures] of Object.entries(PRODUCTS_BY_CATEGORY)) {
    const catIndex = CATEGORY_TREE.findIndex((c) => c.slug === slug);
    const categoryId = categoryIds[catIndex] ?? categoryIds[0];
    for (const fixture of fixtures) {
      const variants = randInt(1, 4);
      const skus: Array<{ sku: string; size?: string; color?: string; stock: number }> = [];
      const sizes  = ["XS","S","M","L","XL","XXL"];
      const colors = ["Black","White","Navy","Red","Green","Grey","Blue","Pink"];
      for (let v = 0; v < variants; v++) {
        skus.push({
          sku: `${slug.toUpperCase().slice(0, 3)}-${randInt(10000, 99999)}-V${v + 1}`,
          ...(slug === "clothing" ? { size: pick(sizes), color: pick(colors) } : {}),
          stock: randInt(0, 500),
        });
      }
      allProducts.push({
        name:        fixture.name,
        slug:        fixture.name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
        category_id: categoryId,
        brand:       pick(BRANDS),
        description: `High-quality ${fixture.name.toLowerCase()}. ${pick(REVIEW_ADJECTIVES)} product loved by customers worldwide.`,
        price:       fixture.basePrice,
        compare_at_price: randBool(0.4) ? parseFloat((fixture.basePrice * randFloat(1.1, 1.5)).toFixed(2)) : null,
        cost:        parseFloat((fixture.basePrice * randFloat(0.3, 0.6)).toFixed(2)),
        currency:    "USD",
        tags:        fixture.tags,
        rating:      randFloat(3.0, 5.0, 1),
        review_count: randInt(0, 800),
        weight_kg:   randFloat(0.1, 15.0),
        active:      randBool(0.88),
        featured:    randBool(0.15),
        skus,
        images: Array.from({ length: randInt(1, 5) }, (_, i) => ({
          url:  `https://example.com/images/${slug}-${i + 1}.jpg`,
          alt:  `${fixture.name} image ${i + 1}`,
          sort: i,
        })),
        meta: {
          created_at: randDate(yearAgo, now),
          updated_at: randDate(yearAgo, now),
          views: randInt(100, 50000),
        },
      });
    }
  }

  const productIds = await batchInsert(db, "products", allProducts);
  console.log(`Inserted ${productIds.length} products with variants.`);

  // ── 4. Coupons ──────────────────────────────────────────────────────────────

  const couponData: Record<string, unknown>[] = [
    { code: "WELCOME10", type: "percent",  value: 10,  min_order: 0,   max_uses: 10000, uses: 3421, active: true  },
    { code: "SAVE20",    type: "percent",  value: 20,  min_order: 50,  max_uses: 5000,  uses: 1200, active: true  },
    { code: "FLAT15",    type: "fixed",    value: 15,  min_order: 75,  max_uses: 2000,  uses: 890,  active: true  },
    { code: "FREESHIP",  type: "shipping", value: 0,   min_order: 30,  max_uses: 50000, uses: 9800, active: true  },
    { code: "VIP50",     type: "percent",  value: 50,  min_order: 200, max_uses: 100,   uses: 77,   active: false },
    { code: "FLASH30",   type: "percent",  value: 30,  min_order: 100, max_uses: 1000,  uses: 1000, active: false },
    { code: "NEWUSER",   type: "percent",  value: 15,  min_order: 0,   max_uses: 0,     uses: 5600, active: true  },
    { code: "SUMMER25",  type: "percent",  value: 25,  min_order: 60,  max_uses: 3000,  uses: 420,  active: true  },
  ].map((c) => ({ ...c, expires_at: randDate(now, new Date(now.getTime() + 180 * 24 * 60 * 60 * 1000)) }));

  const couponIds = await batchInsert(db, "coupons", couponData);
  console.log(`Inserted ${couponIds.length} coupons.`);

  // ── 5. Customers (500) ──────────────────────────────────────────────────────

  const CUSTOMER_COUNT = 500;
  const customerDocs: Record<string, unknown>[] = Array.from({ length: CUSTOMER_COUNT }, (_, i) => {
    const firstName = pick(FIRST_NAMES);
    const lastName  = pick(LAST_NAMES);
    const location  = pick(CITIES);
    return {
      first_name: firstName,
      last_name:  lastName,
      email:      `${firstName.toLowerCase()}.${lastName.toLowerCase()}${i}@example.com`,
      phone:      `+1${randInt(2000000000, 9999999999)}`,
      date_of_birth: randDate(new Date("1960-01-01"), new Date("2002-01-01")).slice(0, 10),
      address: {
        street:  `${randInt(1, 9999)} ${pick(LAST_NAMES)} St`,
        city:    location.city,
        state:   location.state,
        country: location.country,
        zip:     location.zip,
      },
      tier:       pick(["bronze","silver","gold","platinum"]),
      newsletter: randBool(0.6),
      active:     randBool(0.92),
      total_spent: 0,
      order_count: 0,
      tags: randBool(0.3) ? [pick(["vip","wholesale","influencer","loyalty"])] : [],
      created_at: randDate(yearAgo, now),
      last_login: randDate(new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000), now),
    };
  });

  const customerIds = await batchInsert(db, "customers", customerDocs, 50);
  console.log(`Inserted ${customerIds.length} customers.`);

  // ── 6. Orders + Order Items (800 orders) ───────────────────────────────────

  const ORDER_COUNT = 800;
  const orderDocs:     Record<string, unknown>[] = [];
  const orderItemDocs: Record<string, unknown>[] = [];

  for (let o = 0; o < ORDER_COUNT; o++) {
    const customerId   = pick(customerIds);
    const status       = pick(ORDER_STATUSES);
    const itemCount    = randInt(1, 6);
    const orderDate    = randDate(yearAgo, now);
    const location     = pick(CITIES);
    const coupon       = randBool(0.25) ? pick(couponIds) : null;
    const carrier      = pick(CARRIERS);

    // pick random products for this order
    const chosenProducts = Array.from({ length: itemCount }, () => {
      const idx = randInt(0, productIds.length - 1);
      return { id: productIds[idx], data: allProducts[idx] };
    });

    const lineItems = chosenProducts.map((p) => {
      const qty      = randInt(1, 4);
      const price    = (p.data["price"] as number) ?? 19.99;
      const discount = randBool(0.2) ? parseFloat((price * randFloat(0.05, 0.20)).toFixed(2)) : 0;
      return { productId: p.id, qty, price, discount, line_total: parseFloat(((price - discount) * qty).toFixed(2)) };
    });

    const subtotal  = lineItems.reduce((s, l) => s + l.line_total, 0);
    const shipping  = subtotal > 75 ? 0 : randFloat(4.99, 14.99);
    const tax       = parseFloat((subtotal * 0.08).toFixed(2));
    const discount  = coupon ? parseFloat((subtotal * 0.10).toFixed(2)) : 0;
    const total     = parseFloat((subtotal + shipping + tax - discount).toFixed(2));

    const orderId = `ord-${String(o + 1).padStart(6, "0")}`;

    orderDocs.push({
      order_id:   orderId,
      customer_id: customerId,
      status,
      payment_method: pick(PAYMENT_METHODS),
      payment_status: status === "cancelled" ? "refunded" : status === "pending" ? "pending" : "paid",
      coupon_id:  coupon,
      subtotal:   parseFloat(subtotal.toFixed(2)),
      shipping,
      tax,
      discount,
      total,
      item_count: itemCount,
      currency:   "USD",
      shipping_address: {
        street:  `${randInt(1, 9999)} ${pick(LAST_NAMES)} Ave`,
        city:    location.city,
        state:   location.state,
        country: location.country,
        zip:     location.zip,
      },
      carrier,
      tracking_number: status !== "pending" && status !== "processing"
        ? `${carrier.toUpperCase().replace(/\s/g,"").slice(0,3)}${randInt(1e11, 9e11)}`
        : null,
      notes:     randBool(0.1) ? "Please leave at door." : null,
      created_at: orderDate,
      updated_at: orderDate,
    });

    for (const li of lineItems) {
      orderItemDocs.push({
        order_id:   orderId,
        product_id: li.productId,
        quantity:   li.qty,
        unit_price: li.price,
        discount:   li.discount,
        line_total: li.line_total,
      });
    }
  }

  await batchInsert(db, "orders",      orderDocs,      50);
  await batchInsert(db, "order_items", orderItemDocs,  50);
  console.log(`Inserted ${orderDocs.length} orders with ${orderItemDocs.length} line items.`);

  // ── 7. Reviews (1000) ──────────────────────────────────────────────────────

  const REVIEW_COUNT = 1000;
  const reviewDocs: Record<string, unknown>[] = Array.from({ length: REVIEW_COUNT }, () => {
    const rating   = randInt(1, 5);
    const adjective = pick(REVIEW_ADJECTIVES);
    return {
      product_id:  pick(productIds),
      customer_id: pick(customerIds),
      rating,
      title:  `${adjective} product!`,
      body:   `${adjective}. ${rating >= 4 ? "Highly recommend this." : "Could be better."} ${randBool(0.5) ? "Fast shipping." : "Packaging was great."}`,
      verified_purchase: randBool(0.75),
      helpful_votes:     randInt(0, 120),
      images: randBool(0.2) ? [`https://example.com/review-imgs/r${randInt(1,9999)}.jpg`] : [],
      created_at: randDate(yearAgo, now),
    };
  });

  await batchInsert(db, "reviews", reviewDocs, 50);
  console.log(`Inserted ${reviewDocs.length} reviews.`);

  // ── 8. Sample queries ──────────────────────────────────────────────────────

  console.log("\n── Sample queries ────────────────────────────────────────────");

  const q = db.query();

  // Active products priced over $100
  const premiumProducts = await q.find("products", {
    and: [{ eq: ["active", true] }, { gt: ["price", 100] }],
  });
  console.log(`Active products with price > $100: ${premiumProducts.length}`);

  // Featured products
  const featured = await q.find("products", { eq: ["featured", true] });
  console.log(`Featured products: ${featured.length}`);

  // Gold+ tier customers
  const vipCustomers = await q.findIds("customers", {
    in: ["tier", ["gold", "platinum"]],
  });
  console.log(`Gold/Platinum customers: ${vipCustomers.length}`);

  // Delivered orders
  const deliveredIds = await q.findIds("orders", { eq: ["status", "delivered"] });
  console.log(`Delivered orders: ${deliveredIds.length}`);

  // Shipped + paid orders
  const shippedPaid = await q.find("orders", {
    and: [{ eq: ["status", "shipped"] }, { eq: ["payment_status", "paid"] }],
  });
  console.log(`Shipped & paid orders: ${shippedPaid.length}`);

  // 5-star reviews
  const fiveStarIds = await q.findIds("reviews", { eq: ["rating", 5] });
  console.log(`5-star reviews: ${fiveStarIds.length}`);

  // Active coupons
  const activeCoupons = await q.find("coupons", { eq: ["active", true] });
  console.log(`Active coupons: ${activeCoupons.map((c) => c["code"]).join(", ")}`);

  await q.commit();

  // ── Done ──────────────────────────────────────────────────────────────────

  const totalDocs =
    categoryIds.length +
    productIds.length +
    couponIds.length +
    customerIds.length +
    orderDocs.length +
    orderItemDocs.length +
    reviewDocs.length;

  console.log(`\n✓ Demo complete. Total documents inserted: ${totalDocs}`);
  db.close();
}

demo().catch((e) => {
  console.error(e);
  process.exit(1);
});
