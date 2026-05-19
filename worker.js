const WEB_APP_URL = "https://snowmanbot-api.zekobusiness0.workers.dev/";
const HOUR_MS = 60 * 60 * 1000;

let schemaInitialized = false;
let leaderboardCache = null;
let leaderboardCacheTime = 0;
const LEADERBOARD_CACHE_MS = 8 * 60 * 60 * 1000;
const rateLimitMemory = new Map();

// إصلاح دالة json لدعم الـ CORS (ضروري لعمل البوت في المتصفح وتليجرام)
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*", // يسمح بالوصول من أي مصدر
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
      "Cache-Control": "no-store"
    }
  });
}

async function getUserIdFromRequest(request) {
    const initData = request.headers.get('X-Telegram-Init-Data');
    if (!initData) return null;
    const params = new URLSearchParams(initData);
    const userStr = params.get('user');
    if (!userStr) return null;
    try {
        return JSON.parse(userStr).id;
    } catch { return null; }
}

async function requireSessionUser(request, env, bodyUserId = null) {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return { ok: false, error: "Unauthorized" };

  const sessionUserId = await getUserIdFromRequest(request);
  if (!sessionUserId) return { ok: false, error: "Unauthorized" };

  if (bodyUserId !== null && Number(bodyUserId) !== sessionUserId) {
    return { ok: false, error: "Unauthorized" };
  }

  return { ok: true, userId: sessionUserId };
}

function isValidTonAddress(addr) {
    return typeof addr === 'string' &&
           addr.length >= 48 &&
           addr.length <= 66 &&
           /^[A-Za-z0-9_\-+/=]+$/.test(addr);
}

async function verifyTelegramAuth(request, env) {
  const initData = request.headers.get('X-Telegram-Init-Data');
  if (!initData) return false;
  
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  params.delete('hash');
  
  const dataCheckString = [...params.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join('\n');
  
  const encoder = new TextEncoder();
  const secretKey = await crypto.subtle.importKey(
    'raw', encoder.encode('WebAppData'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const botKeyBytes = await crypto.subtle.sign('HMAC', secretKey, encoder.encode(env.BOT_TOKEN));
  const dataKey = await crypto.subtle.importKey(
    'raw', botKeyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign('HMAC', dataKey, encoder.encode(dataCheckString));
  const expectedHash = Array.from(new Uint8Array(signature))
    .map(b => b.toString(16).padStart(2, '0')).join('');
  
  if (expectedHash !== hash) return false;
  
  const authDate = Number(params.get('auth_date'));
  if (Date.now() / 1000 - authDate > 86400) return false;
  
  return true;
}

async function checkRateLimit(env, userId, action, maxPerMinute = 10) {
    const key = `${userId}:${action}`;
    const now = Date.now();
    const windowMs = 60000;

    const existing = rateLimitMemory.get(key);

    if (!existing || now - existing.windowStart > windowMs) {
        rateLimitMemory.set(key, { count: 1, windowStart: now });
        return true;
    }

    if (existing.count >= maxPerMinute) return false;

    existing.count++;
    return true;
}

async function ensureSchema(env) {
  if (schemaInitialized) return;
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS users (
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      display_name TEXT,
      snow_balance REAL NOT NULL DEFAULT 0,
      snowman_count INTEGER NOT NULL DEFAULT 0,
      mining_boost REAL NOT NULL DEFAULT 1,
      last_mined_at INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL DEFAULT 0
    )
  `).run();

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS tasks (
      task_id INTEGER PRIMARY KEY AUTOINCREMENT,
      creator_user_id INTEGER NOT NULL,
      source_type TEXT NOT NULL,
      title TEXT NOT NULL,
      link TEXT NOT NULL,
      target_users INTEGER NOT NULL,
      cost_snow REAL NOT NULL DEFAULT 0,
      reward_snow REAL NOT NULL DEFAULT 2,
      completion_limit INTEGER NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'under_review',
      channel_message_id INTEGER,
      created_at INTEGER NOT NULL,
      approved_at INTEGER,
      published_at INTEGER,
      rejected_at INTEGER
    )
  `).run();

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS task_completions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      completed_at INTEGER NOT NULL,
      UNIQUE(task_id, user_id)
    )
  `).run();

  await env.DB.prepare(
    `ALTER TABLE users ADD COLUMN ton_balance REAL NOT NULL DEFAULT 0`
  ).run().catch(() => {});

  await env.DB.prepare(
    `ALTER TABLE users ADD COLUMN wallet_address TEXT`
  ).run().catch(() => {});

  await env.DB.prepare(`
      CREATE TABLE IF NOT EXISTS ton_deposits (
        tx_hash TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        created_at INTEGER NOT NULL
      )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS boost_purchases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      pack TEXT NOT NULL,
      boost_factor REAL NOT NULL,
      ton_cost REAL NOT NULL,
      purchased_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

  await env.DB.prepare(
    `ALTER TABLE users ADD COLUMN referred_by INTEGER`
  ).run().catch(() => {});

  await env.DB.prepare(
    `ALTER TABLE users ADD COLUMN referral_ton_earned REAL NOT NULL DEFAULT 0`
  ).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      fee REAL NOT NULL,
      net_amount REAL NOT NULL,
      ton_address TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      message_id INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS market_listings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      seller_id INTEGER NOT NULL,
      snow_amount REAL NOT NULL,
      price_per_snow REAL NOT NULL,
      total_price REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS pvp_rounds (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      status TEXT NOT NULL DEFAULT 'waiting',
      total_pot REAL NOT NULL DEFAULT 0,
      winner_id INTEGER,
      winner_amount REAL,
      winner_share REAL,
      started_at INTEGER,
      locked_at INTEGER,
      ended_at INTEGER,
      created_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS pvp_bets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      round_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      username TEXT,
      display_name TEXT,
      amount REAL NOT NULL,
      created_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS promo_codes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE,
      reward_snow REAL NOT NULL DEFAULT 0,
      max_uses INTEGER NOT NULL DEFAULT 100,
      uses_count INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL DEFAULT 0
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS promo_uses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL,
      user_id INTEGER NOT NULL,
      used_at INTEGER NOT NULL,
      UNIQUE(code, user_id)
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS rate_limits (
      key TEXT PRIMARY KEY,
      count INTEGER NOT NULL DEFAULT 1,
      window_start INTEGER NOT NULL
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS lootbox_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      box_type TEXT NOT NULL,
      reward_type TEXT NOT NULL,
      reward_amount REAL NOT NULL,
      created_at INTEGER NOT NULL
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS app_stats (
      key TEXT PRIMARY KEY,
      value REAL NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL DEFAULT 0
    )
  `).run().catch(() => {});

  await env.DB.prepare(`
    INSERT OR IGNORE INTO app_stats (key, value, updated_at)
    VALUES ('total_snowmen', 0, 0)
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TRIGGER IF NOT EXISTS trg_users_total_snowmen_insert
    AFTER INSERT ON users
    BEGIN
      UPDATE app_stats
      SET value = value + COALESCE(NEW.snowman_count, 0),
          updated_at = CAST(strftime('%s','now') AS INTEGER) * 1000
      WHERE key = 'total_snowmen';
    END
  `).run().catch(() => {});

  await env.DB.prepare(`
    CREATE TRIGGER IF NOT EXISTS trg_users_total_snowmen_update
    AFTER UPDATE OF snowman_count ON users
    BEGIN
      UPDATE app_stats
      SET value = value + (COALESCE(NEW.snowman_count, 0) - COALESCE(OLD.snowman_count, 0)),
          updated_at = CAST(strftime('%s','now') AS INTEGER) * 1000
      WHERE key = 'total_snowmen';
    END
  `).run().catch(() => {});

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_users_snowman_count ON users(snowman_count DESC)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_task_completions_user ON task_completions(user_id)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_task_completions_task ON task_completions(task_id)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_listings_status_created ON market_listings(status, created_at DESC)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_withdrawals_user_created ON withdrawals(user_id, created_at DESC)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_ton_deposits_user_created ON ton_deposits(user_id, created_at DESC)`).run().catch(() => {});
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_pvp_rounds_status_id ON pvp_rounds(status, id DESC)`).run().catch(() => {});

  const statsRow = await env.DB.prepare(`SELECT updated_at FROM app_stats WHERE key = 'total_snowmen'`).first();
  if (!statsRow || Number(statsRow.updated_at || 0) === 0) {
    const totalRow = await env.DB.prepare(`SELECT COALESCE(SUM(snowman_count), 0) as total FROM users`).first();
    await env.DB.prepare(`UPDATE app_stats SET value = ?, updated_at = ? WHERE key = 'total_snowmen'`)
      .bind(Number(totalRow?.total || 0), Date.now())
      .run();
  }
  schemaInitialized = true;
}

async function getPvpBets(env, roundId) {
  const result = await env.DB.prepare(
    `SELECT user_id, username, display_name, SUM(amount) as amount
     FROM pvp_bets WHERE round_id = ?
     GROUP BY user_id ORDER BY MIN(created_at) ASC`
  ).bind(roundId).all();
  return result.results || [];
}

async function getOrCreatePvpRound(env) {
  const now = Date.now();
  let round = await env.DB.prepare(
    `SELECT * FROM pvp_rounds WHERE status IN ('waiting','countdown') ORDER BY id DESC LIMIT 1`
  ).first();
  if (!round) {
    const res = await env.DB.prepare(
      `INSERT INTO pvp_rounds (status, total_pot, created_at) VALUES ('waiting', 0, ?)`
    ).bind(now).run();
    round = await env.DB.prepare(`SELECT * FROM pvp_rounds WHERE id = ?`).bind(res.meta.last_row_id).first();
  }
  return round;
}

async function processPvpWinner(env, round, bets) {
  const total = bets.reduce((s, b) => s + Number(b.amount), 0);
  if (total === 0 || bets.length < 1) return null;
  const fee = total * 0.15;
  const prize = parseFloat((total - fee).toFixed(2));
  let cumulative = 0;
  const rand = Math.random() * total;
  let winner = bets[bets.length - 1];
  for (const bet of bets) {
    cumulative += Number(bet.amount);
    if (rand <= cumulative) { winner = bet; break; }
  }
  const winnerShare = parseFloat(((Number(winner.amount) / total) * 100).toFixed(2));
  const now = Date.now();
  await env.DB.prepare(
    `UPDATE pvp_rounds SET status='finished', winner_id=?, winner_amount=?, winner_share=?, ended_at=?, total_pot=? WHERE id=?`
  ).bind(winner.user_id, prize, winnerShare, now, total, round.id).run();
  await env.DB.prepare(
    `UPDATE users SET snow_balance = snow_balance + ?, updated_at = ? WHERE user_id = ?`
  ).bind(prize, now, winner.user_id).run();
  return { winner, prize, winnerShare };
}

async function getUser(env, userId) {
  return await env.DB
    .prepare(
      `SELECT user_id, username, display_name, snow_balance, snowman_count,
              mining_boost, last_mined_at, updated_at, ton_balance
       FROM users WHERE user_id = ?`
    )
    .bind(userId)
    .first();
}

async function getTotalSnowmen(env) {
  const row = await env.DB.prepare(
    `SELECT value FROM app_stats WHERE key = 'total_snowmen'`
  ).first();
  if (row && Number.isFinite(Number(row.value))) {
    return Number(row.value || 0);
  }
  const totalRow = await env.DB.prepare(
    `SELECT COALESCE(SUM(snowman_count), 0) as total FROM users`
  ).first();
  return Number(totalRow?.total || 0);
}

async function createUserIfMissing(env, userId, username = null, displayName = null) {
  const existing = await getUser(env, userId);
  if (existing) return existing;

  const now = Date.now();
  await env.DB
    .prepare(
      `INSERT INTO users (user_id, username, display_name, snow_balance, snowman_count, mining_boost, last_mined_at, updated_at)
       VALUES (?, ?, ?, 0, 0, 1, ?, ?)`
    )
    .bind(userId, username, displayName, now, now)
    .run();

  return await getUser(env, userId);
}

function taskTitleFromLink(link) {
  try {
    const url = new URL(link);
    const path = url.pathname.replace(/^\/+/, "");
    if (path) return path.replace(/[\/_]/g, " ");
  } catch (e) {}
  return link.replace(/^https?:\/\//, "").replace(/^t\.me\//, "").replace(/[\/_]/g, " ");
}

async function deductSnow(env, userId, amount) {
  const user = await getUser(env, userId);
  if (!user) throw new Error("User not found");

  const balance = Number(user.snow_balance || 0);
  if (balance < amount) throw new Error("Not enough Snow");

  await env.DB
    .prepare(
      `UPDATE users
       SET snow_balance = snow_balance - ?,
           updated_at = ?
       WHERE user_id = ?`
    )
    .bind(amount, Date.now(), userId)
    .run();
}

async function createTask(env, data) {
  const now = Date.now();
  const result = await env.DB
    .prepare(
      `INSERT INTO tasks (
        creator_user_id, source_type, title, link, target_users,
        cost_snow, reward_snow, completion_limit, status, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, 2, ?, 'under_review', ?)`
    )
    .bind(
      data.creatorUserId,
      data.sourceType,
      data.title,
      data.link,
      data.targetUsers,
      data.costSnow,
      data.completionLimit,
      now
    )
    .run();

  return result.meta.last_row_id;
}

async function getTask(env, taskId) {
  return await env.DB
    .prepare(`SELECT * FROM tasks WHERE task_id = ?`)
    .bind(taskId)
    .first();
}

async function getTasksByCreator(env, creatorUserId) {
  return await env.DB
    .prepare(
      `SELECT
        t.*,
        COALESCE((
          SELECT COUNT(*)
          FROM task_completions tc
          WHERE tc.task_id = t.task_id
        ), 0) AS completion_count
       FROM tasks t
       WHERE t.creator_user_id = ?
       ORDER BY t.created_at DESC`
    )
    .bind(creatorUserId)
    .all();
}

async function getOnlineTasks(env) {
  return await env.DB
    .prepare(
      `SELECT
        t.*,
        COALESCE((
          SELECT COUNT(*)
          FROM task_completions tc
          WHERE tc.task_id = t.task_id
        ), 0) AS completion_count
       FROM tasks t
       WHERE t.status = 'online'
       ORDER BY t.created_at DESC`
    )
    .all();
}

async function sendTaskReviewToChannel(env, task) {
  const creator = await getUser(env, task.creator_user_id);
  const creatorName = creator?.username ? `@${creator.username}` : (creator?.display_name || String(task.creator_user_id));

  const text = [
    "New task pending review",
    "",
    `Title: ${task.title}`,
    `Link: ${task.link}`,
    `Target users: ${task.target_users}`,
    `Completion limit: ${task.completion_limit}`,
    `Cost: ${task.cost_snow} Snow`,
    `Task ID: ${task.task_id}`,
    `Creator: ${creatorName}`
  ].join("\n");

  const response = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: "@snowchannels",
      text,
      reply_markup: {
        inline_keyboard: [[
          { text: "Approve", callback_data: `task_approve:${task.task_id}` },
          { text: "Reject", callback_data: `task_reject:${task.task_id}` }
        ]]
      }
    })
  });

  const data = await response.json();
  return data.result?.message_id || null;
}

async function updateChannelTaskMessage(env, messageId, text) {
  await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: "@snowchannels",
      message_id: messageId,
      text
    })
  });
}

async function handleTaskCreate(env, userId, body, sourceType) {
  await ensureSchema(env);

  const link = String(body.link || "").trim();
  const targetUsers = Math.max(30, Number(body.target_users || 30));
  const completionLimit = sourceType === "submit_channel" ? 250 : targetUsers;
  const title = taskTitleFromLink(link);

  if (!userId || !link) throw new Error("Missing user_id or link");

  await createUserIfMissing(env, userId);

  let costSnow = 0;
  if (sourceType === "add_task") {
    costSnow = Math.round((targetUsers * 100) / 30);
    await deductSnow(env, userId, costSnow);
  }

  const taskId = await createTask(env, {
    creatorUserId: userId,
    sourceType,
    title,
    link,
    targetUsers,
    costSnow,
    completionLimit
  });

  const task = await getTask(env, taskId);
  const messageId = await sendTaskReviewToChannel(env, task);

  if (messageId) {
    await env.DB
      .prepare(`UPDATE tasks SET channel_message_id = ? WHERE task_id = ?`)
      .bind(messageId, taskId)
      .run();
  }

  return {
    success: true,
    task_id: taskId,
    status: "under_review"
  };
}

async function handleTaskApproval(env, taskId, approved) {
  await ensureSchema(env);

  const task = await getTask(env, taskId);
  if (!task) throw new Error("Task not found");

  const now = Date.now();

  if (approved) {
    await env.DB
      .prepare(
        `UPDATE tasks
         SET status = 'online',
             approved_at = ?,
             published_at = ?
         WHERE task_id = ?`
      )
      .bind(now, now, taskId)
      .run();

    if (task.channel_message_id) {
      await updateChannelTaskMessage(
        env,
        task.channel_message_id,
        [
          "Task approved",
          "",
          `Title: ${task.title}`,
          `Link: ${task.link}`,
          `Target users: ${task.target_users}`,
          `Completion limit: ${task.completion_limit}`,
          `Cost: ${task.cost_snow} Snow`,
          `Task ID: ${task.task_id}`,
          "",
          "Status: online"
        ].join("\n")
      );
    }
  } else {
    await env.DB
      .prepare(
        `UPDATE tasks
         SET status = 'rejected',
             rejected_at = ?
         WHERE task_id = ?`
      )
      .bind(now, taskId)
      .run();

    if (task.channel_message_id) {
      await updateChannelTaskMessage(
        env,
        task.channel_message_id,
        [
          "Task rejected",
          "",
          `Title: ${task.title}`,
          `Link: ${task.link}`,
          `Target users: ${task.target_users}`,
          `Completion limit: ${task.completion_limit}`,
          `Cost: ${task.cost_snow} Snow`,
          `Task ID: ${task.task_id}`,
          "",
          "Status: rejected"
        ].join("\n")
      );
    }
  }
}

async function handleTaskComplete(env, userId, body) {
  await ensureSchema(env);

  const taskId = Number(body.task_id);

  if (!userId || !taskId) {
    throw new Error("Missing user_id or task_id");
  }

  const task = await getTask(env, taskId);
  if (!task) {
    throw new Error("Task not found");
  }

  if (task.status !== "online") {
    throw new Error("Task is not online");
  }

  const currentCountRow = await env.DB
    .prepare(`SELECT COUNT(*) AS total FROM task_completions WHERE task_id = ?`)
    .bind(taskId)
    .first();

  const currentCount = Number(currentCountRow?.total || 0);
  const limit = Number(task.completion_limit || 1);

  if (currentCount >= limit) {
    await env.DB
      .prepare(
        `UPDATE tasks
         SET status = 'completed'
         WHERE task_id = ? AND status = 'online'`
      )
      .bind(taskId)
      .run();

    return { success: false, full: true };
  }

  await createUserIfMissing(env, userId);

  try {
    await env.DB
      .prepare(
        `INSERT INTO task_completions (task_id, user_id, completed_at)
         VALUES (?, ?, ?)`
      )
      .bind(taskId, userId, Date.now())
      .run();
  } catch (e) {
    return { success: false, already_done: true };
  }

  await env.DB
    .prepare(
      `UPDATE users
       SET snow_balance = snow_balance + 2,
           updated_at = ?
       WHERE user_id = ?`
    )
    .bind(Date.now(), userId)
    .run();

  const afterCountRow = await env.DB
    .prepare(`SELECT COUNT(*) AS total FROM task_completions WHERE task_id = ?`)
    .bind(taskId)
    .first();

  const afterCount = Number(afterCountRow?.total || 0);

  if (afterCount >= limit) {
    await env.DB
      .prepare(
        `UPDATE tasks
         SET status = 'completed'
         WHERE task_id = ? AND status = 'online'`
      )
      .bind(taskId)
      .run();
  }

  return {
    success: true,
    reward: 2,
    completions: afterCount
  };
}

function computeMiningState(user, now = Date.now()) {
  const snowmanCount = Number(user.snowman_count || 0);
  const miningBoost = Number(user.mining_boost || 1);
   // القاعدة كما هي: تحتاج 350 لتبدأ التعدين (سرعة 1/ساعة)
  const baseSpeed = snowmanCount * 0;
  const speedPerHour = baseSpeed * miningBoost;

  const lastMinedAt = Number(user.last_mined_at || now);
  const elapsed = Math.max(0, now - lastMinedAt);
  const fullHours = Math.floor(elapsed / HOUR_MS);
  const earnedNow = fullHours * speedPerHour;

  const nextLastMinedAt = lastMinedAt + (fullHours * HOUR_MS);
  const remainder = elapsed % HOUR_MS;
  const nextRewardInMs = speedPerHour > 0 ? HOUR_MS - remainder : 0;

  return {
    speedPerHour,
    earnedNow,
    nextLastMinedAt,
    nextRewardInMs
  };
}

async function settleUserMining(env, userId, username = null, displayName = null) {
  await ensureSchema(env);
  let user = await createUserIfMissing(env, userId, username, displayName);
  const now = Date.now();
  const computed = computeMiningState(user, now);  

  if (computed.earnedNow > 0) {
    await env.DB
      .prepare(
        `UPDATE users
         SET snow_balance = snow_balance + ?,
             last_mined_at = ?,
             updated_at = ?
         WHERE user_id = ?`
      )
      .bind(computed.earnedNow, computed.nextLastMinedAt, now, userId)
      .run();

    user = await getUser(env, userId);

  } else if (!user.last_mined_at || user.last_mined_at === 0) {
    await env.DB
      .prepare(
        `UPDATE users
         SET last_mined_at = ?, updated_at = ?
         WHERE user_id = ?`
      )
      .bind(now, now, userId)
      .run();

    user = await getUser(env, userId);
  }

  const finalComputed = computeMiningState(user, now);

return {
  user: {
    user_id: Number(user.user_id),
    username: user.username || null,
    display_name: user.display_name || null,
    snow_balance: Number(user.snow_balance || 0),
    snowman_count: Number(user.snowman_count || 0),
    mining_boost: Number(user.mining_boost || 1),
    last_mined_at: Number(user.last_mined_at || 0),
    speed_per_hour: finalComputed.speedPerHour,
    earned_now: finalComputed.earnedNow,
    next_reward_in_ms: finalComputed.nextRewardInMs,
    ton_balance: Number(user.ton_balance || 0),
  },
  server_time: now
};
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // معالجة طلبات OPTIONS (مهمة جداً للمتصفحات لتجنب أخطاء الـ CORS)
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
        },
      });
    }

    if (url.pathname === "/telegram" && request.method === "POST") {
      try {
        const update = await request.json();
        const text = update.message?.text;
        const chatId = update.message?.chat?.id;
        
if (update.callback_query) {
  const callbackQuery = update.callback_query;
  const callbackData = callbackQuery.data || "";
  const callbackUserId = callbackQuery.from?.id;
  const messageId = callbackQuery.message?.message_id;
  const chatIdCallback = callbackQuery.message?.chat?.id;

  const adminChannelId = Number(env.ADMIN_CHANNEL_ID);
  const isAdmin = callbackUserId === adminChannelId || 
    (await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getChatMember?chat_id=${env.ADMIN_CHANNEL_ID}&user_id=${callbackUserId}`)
      .then(r => r.json())
      .then(d => ["creator","administrator"].includes(d.result?.status))
      .catch(() => false));

  if (!isAdmin) {
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Not authorized." })
    });
    return new Response("ok");
  }

  if (callbackData.startsWith("approve_")) {
    const withdrawId = Number(callbackData.replace("approve_", ""));
    const withdrawal = await env.DB.prepare(
      `SELECT * FROM withdrawals WHERE id = ?`
    ).bind(withdrawId).first();

    if (withdrawal && withdrawal.status === "pending") {
  await env.DB.prepare(
    `UPDATE withdrawals SET status = 'completed', updated_at = ? WHERE id = ?`
  ).bind(Date.now(), withdrawId).run();

  const wUser = await getUser(env, withdrawal.user_id);
  const wName = wUser?.display_name || wUser?.username || String(withdrawal.user_id);

  await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatIdCallback,
      message_id: messageId,
      text: `✅ Withdrawal Completed\n\n👤 User: ${wName}\n💎 Net Amount: ${withdrawal.net_amount} TON\n🏦 Status: Paid\nTon Address: ${withdrawal.ton_address}\n\n☃️ SnowManBot — Play & Earn TON\n👉 @Snow0ManBot`
    })
  });

  await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: withdrawal.user_id,
      text: `✅ Your withdrawal of ${withdrawal.net_amount} TON has been completed and sent to your wallet.`
    })
  });
}

    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Approved!" })
    });
  }

if (callbackData.startsWith("task_approve:") || callbackData.startsWith("task_reject:")) {
    const isTaskAdmin = await fetch(
      `https://api.telegram.org/bot${env.BOT_TOKEN}/getChatMember?chat_id=@snowchannels&user_id=${callbackUserId}`
    ).then(r => r.json())
     .then(d => ["creator","administrator"].includes(d.result?.status))
     .catch(() => false);

    if (!isTaskAdmin) {
      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Not authorized." })
      });
      return new Response("ok");
    }

    const [action, taskIdRaw] = callbackData.split(":");
    const taskId = Number(taskIdRaw);
    try {
      if (action === "task_approve") {
        await handleTaskApproval(env, taskId, true);
      } else if (action === "task_reject") {
        await handleTaskApproval(env, taskId, false);
      }
      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Updated" })
      });
    } catch (e) {
      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Failed" })
      });
    }
    return new Response("ok");
  }
  
  if (callbackData.startsWith("reject_")) {
    const withdrawId = Number(callbackData.replace("reject_", ""));
    const withdrawal = await env.DB.prepare(
      `SELECT * FROM withdrawals WHERE id = ?`
    ).bind(withdrawId).first();

    if (withdrawal && withdrawal.status === "pending") {
      await env.DB.prepare(
        `UPDATE withdrawals SET status = 'rejected', updated_at = ? WHERE id = ?`
      ).bind(Date.now(), withdrawId).run();

      await env.DB.prepare(
        `UPDATE users SET ton_balance = ton_balance + ?, updated_at = ? WHERE user_id = ?`
      ).bind(withdrawal.amount, Date.now(), withdrawal.user_id).run();

      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/editMessageText`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatIdCallback,
          message_id: messageId,
          text: `❌ Withdrawal Rejected\n\n👤 Ton Address: ${withdrawal.ton_address}\n💎 Amount Refunded: ${withdrawal.amount} TON\n\n☃️ SnowManBot — Play & Earn TON\n👉 @Snow0ManBot`
        })
      });

      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: withdrawal.user_id,
          text: `❌ Your withdrawal of ${withdrawal.amount} TON was rejected. The amount has been refunded to your balance.`
        })
      });
    }

    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ callback_query_id: callbackQuery.id, text: "Rejected." })
    });
  }

  return new Response("ok");
}
        
      if (text && text.startsWith("/start") && chatId) {
  const parts = text.split(" ");
  const refId = parts[1] ? Number(parts[1]) : null;

  if (refId && refId !== chatId) {
    await ensureSchema(env);
    const newUser = await createUserIfMissing(env, chatId);
    if (newUser && !newUser.referred_by) {
      await env.DB.prepare(
        `UPDATE users SET referred_by = ? WHERE user_id = ? AND referred_by IS NULL`
      ).bind(refId, chatId).run();

      await env.DB.prepare(
        `UPDATE users SET snowman_count = snowman_count + 1, last_mined_at = ?, updated_at = ? WHERE user_id = ?`
      ).bind(Date.now(), Date.now(), refId).run();
    }
  }

  await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              chat_id: chatId,
              text: "Welcome to SnowManBot Empire ☃️",
              reply_markup: {
                inline_keyboard: [
  [
    { text: "Open", web_app: { url: WEB_APP_URL } }
  ],
  [
    { text: "Join the Channel", url: "https://t.me/SnowManNew" }
  ],
  [
    { text: "Chat", url: "https://t.me/Snow0ManChat" }
  ]
]
              }
            })
          });
        }
        
      } catch (e) {}
      return new Response("ok");
    }

if (url.pathname === "/api/friends" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const userId = Number(url.searchParams.get("user_id"));
    if (!userId) return json({ error: "Missing user_id" }, 400);

    const friends = await env.DB.prepare(
  `SELECT user_id, username, display_name FROM users WHERE referred_by = ? ORDER BY updated_at DESC`
).bind(userId).all();

    const rewardRow = await env.DB.prepare(
      `SELECT referral_ton_earned FROM users WHERE user_id = ?`
    ).bind(userId).first();

    return json({
      friends: friends.results || [],
      count: (friends.results || []).length,
      ton_earned: Number(rewardRow?.referral_ton_earned || 0)
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}
    
    if (url.pathname === "/api/status" && request.method === "GET") {
  return json({ status: "ok", message: "SnowmanBot API is running" });
}

if (url.pathname === "/api/ton/check" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const sessionUserId = await getUserIdFromRequest(request);
if (!sessionUserId) return json({ error: "Unauthorized" }, 401);
const userId = sessionUserId;
    const amount = Number(url.searchParams.get("amount"));
if (!Number.isFinite(amount) || amount <= 0) {
  return json({ error: "Invalid amount" }, 400);
}

if (!await checkRateLimit(env, userId, "ton_check", 3)) {
  return json({ error: "Too many requests" }, 429);
}

const DEPOSIT_WALLET = "UQBJCCvVCXWXJ5pDAlCT4R4ew-k4WNdaigTMjJ-pP_RxbTqq";
const res = await fetch(
  `https://toncenter.com/api/v2/getTransactions?address=${DEPOSIT_WALLET}&limit=20`
);
const data = await res.json();
const transactions = data.result || [];
const expectedNano = Math.round(amount * 1e9);
const fiveMinAgo = Math.floor(Date.now() / 1000) - 300;

const match = transactions.find(tx => {
  const inMsg = tx.in_msg;
  if (!inMsg) return false;
  const comment = inMsg.message || "";
  const value = Number(inMsg.value || 0);
  const time = Number(tx.utime || 0);
  const validTime = time >= fiveMinAgo;
  const validAmount = value >= expectedNano * 0.98;
  const byComment = comment === String(userId);
  return validTime && validAmount && byComment;
});

if (!match) return json({ found: false });

const txHash = match.transaction_id?.hash || "";
if (!txHash) {
  return json({ error: "Invalid transaction" }, 400);
}

const insertResult = await env.DB.prepare(
  `INSERT OR IGNORE INTO ton_deposits (tx_hash, user_id, amount, created_at)
   VALUES (?, ?, ?, ?)`
)
  .bind(txHash, userId, amount, Date.now())
  .run();

if (insertResult.meta.changes === 0) {
  return json({ found: true, already_credited: true });
}

await env.DB.prepare(
  `UPDATE users SET ton_balance = ton_balance + ?, updated_at = ? WHERE user_id = ?`
).bind(amount, Date.now(), userId).run();
    
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: userId,
        text: `Deposit confirmed! ${amount} TON has been added to your SnowMan Empire account.`
      })
    });

    return json({ found: true, amount });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/tonconnect-manifest.json") {
  return new Response(JSON.stringify({
    url: "https://snowmanbot-api.zekobusiness0.workers.dev",
    name: "SnowMan Empire",
    iconUrl: "https://raw.githubusercontent.com/zekogg/SnowMan-images/refs/heads/main/SnowMan%20Background%20photo.webp"
  }), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
  });
}

if (url.pathname === "/api/boost" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const body = await request.json();
    const sessionUserId = await getUserIdFromRequest(request);
    const userId = sessionUserId || Number(body.user_id);
    if (!sessionUserId || sessionUserId !== Number(body.user_id)) {
      return json({ error: "Unauthorized" }, 401);
    }
    const pack = String(body.pack || "").trim();

    if (!await checkRateLimit(env, userId, 'boost', 5)) {
      return json({ error: "Too many requests" }, 429);
    }

    const PACKS = {
      boost2x: { ton: 1, factor: 2 },
      boost10x: { ton: 4, factor: 10 }
    };

    if (!userId || !PACKS[pack]) return json({ error: "Invalid params" }, 400);

    const user = await getUser(env, userId);
    if (!user) return json({ error: "User not found" }, 404);

    const cost = PACKS[pack].ton;
    const factor = PACKS[pack].factor;
    const now = Date.now();

    await settleUserMining(env, userId);

    const boostResult = await env.DB.prepare(
      `UPDATE users
       SET ton_balance = ton_balance - ?,
           mining_boost = COALESCE(mining_boost, 1) * ?,
           last_mined_at = ?,
           updated_at = ?
       WHERE user_id = ? AND ton_balance >= ?`
    ).bind(cost, factor, now, now, userId, cost).run();

    if (boostResult.meta.changes === 0) return json({ error: "Not enough TON" }, 400);

    await env.DB.prepare(
      `INSERT INTO boost_purchases (user_id, pack, boost_factor, ton_cost, purchased_at) VALUES (?, ?, ?, ?, ?)`
    ).bind(userId, pack, factor, cost, now).run();

    const buyer = await getUser(env, userId);
    if (buyer?.referred_by) {
      const refReward = cost * 0.15;
      await env.DB.prepare(
        `UPDATE users SET ton_balance = ton_balance + ?, referral_ton_earned = referral_ton_earned + ?, updated_at = ? WHERE user_id = ?`
      ).bind(refReward, refReward, now, buyer.referred_by).run();

      await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: buyer.referred_by,
          text: `Your friend bought a booster! You earned ${refReward.toFixed(2)} TON referral bonus.`
        })
      });
    }

    const updatedUser = await getUser(env, userId);
    const computed = computeMiningState(updatedUser, now);

    return json({
      ok: true,
      pack,
      factor,
      ton_cost: cost,
      speed_per_hour: computed.speedPerHour,
      user: {
        user_id: Number(updatedUser.user_id),
        username: updatedUser.username || null,
        display_name: updatedUser.display_name || null,
        snow_balance: Number(updatedUser.snow_balance || 0),
        snowman_count: Number(updatedUser.snowman_count || 0),
        mining_boost: Number(updatedUser.mining_boost || 1),
        last_mined_at: Number(updatedUser.last_mined_at || 0),
        updated_at: Number(updatedUser.updated_at || 0),
        ton_balance: Number(updatedUser.ton_balance || 0)
      }
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}if (url.pathname === "/api/pvp/current" && request.method === "GET") {
  try {
    await ensureSchema(env);
    const now = Date.now();
    let round = await getOrCreatePvpRound(env);
    let winner = null;
    let timeLeft = null;

    if (round.status === 'countdown' && round.started_at) {
      const elapsed = Math.floor((now - round.started_at) / 1000);
      timeLeft = Math.max(0, 20 - elapsed);

      if (timeLeft <= 2 && !round.locked_at) {
        await env.DB.prepare(`UPDATE pvp_rounds SET locked_at=? WHERE id=?`).bind(now, round.id).run();
        round = { ...round, locked_at: now };
      }

      if (timeLeft === 0) {
        const bets = await getPvpBets(env, round.id);
        const result = await processPvpWinner(env, round, bets);
        if (result) {
          winner = result;
          round = { ...round, status: 'finished' };
          const newRes = await env.DB.prepare(
            `INSERT INTO pvp_rounds (status, total_pot, created_at) VALUES ('waiting', 0, ?)`
          ).bind(now).run();
          const newRound = await env.DB.prepare(`SELECT * FROM pvp_rounds WHERE id=?`).bind(newRes.meta.last_row_id).first();
          const bets2 = await getPvpBets(env, round.id);
          return json({ round, bets: bets2, time_left: 0, locked: true, winner, server_time: now, new_round: newRound });
        }
      }
    }

    const bets = await getPvpBets(env, round.id);
    return json({ round, bets, time_left: timeLeft, locked: !!round.locked_at, winner, server_time: now });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/pvp/bet" && request.method === "POST") {
  try {
    await ensureSchema(env);
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const userId = auth.userId;
    const amount = Number(body.amount);
    if (!userId || amount < 50) return json({ error: "Minimum bet is 50 Snow" }, 400);

    if (!await checkRateLimit(env, userId, "pvp_bet", 5)) {
      return json({ error: "Too many requests" }, 429);
    }

    const user = await getUser(env, userId);
    if (!user) return json({ error: "User not found" }, 404);

    const now = Date.now();
    let round = await getOrCreatePvpRound(env);

    if (round.locked_at) return json({ error: "Bets are locked" }, 400);
    if (round.status === "countdown" && round.started_at) {
      const elapsed = (now - round.started_at) / 1000;
      if (elapsed >= 18) return json({ error: "Bets are locked" }, 400);
    }

    const spend = await env.DB.prepare(
      `UPDATE users
       SET snow_balance = snow_balance - ?, updated_at = ?
       WHERE user_id = ? AND snow_balance >= ?`
    ).bind(amount, now, userId, amount).run();

    if (spend.meta.changes === 0) return json({ error: "Not enough Snow" }, 400);

    await env.DB.prepare(
      `INSERT INTO pvp_bets (round_id, user_id, username, display_name, amount, created_at) VALUES (?,?,?,?,?,?)`
    ).bind(round.id, userId, user.username, user.display_name, amount, now).run();

    await env.DB.prepare(
      `UPDATE pvp_rounds SET total_pot = total_pot + ? WHERE id = ?`
    ).bind(amount, round.id).run();

    const bets = await getPvpBets(env, round.id);
    const uniqueUsers = new Set(bets.map(b => b.user_id)).size;
    if (uniqueUsers >= 1 && round.status === "waiting") {
      await env.DB.prepare(
        `UPDATE pvp_rounds SET status='countdown', started_at=? WHERE id=?`
      ).bind(now, round.id).run();
    }

    return json({ ok: true });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/promo/redeem" && request.method === "POST") {
  try {
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const userId = auth.userId;
    const code = String(body.code || "").trim().toUpperCase();

    if (!userId || !code) return json({ error: "Missing params" }, 400);

    const promo = await env.DB.prepare(
      `SELECT * FROM promo_codes WHERE code = ?`
    ).bind(code).first();

    if (!promo) return json({ error: "Invalid code" }, 400);

    const alreadyUsed = await env.DB.prepare(
      `SELECT id FROM promo_uses WHERE code = ? AND user_id = ?`
    ).bind(code, userId).first();
    if (alreadyUsed) return json({ error: "You already used this code" }, 400);

    const now = Date.now();

    await env.DB.prepare(
      `UPDATE users SET snow_balance = snow_balance + ?, updated_at = ? WHERE user_id = ?`
    ).bind(promo.reward_snow, now, userId).run();

    await env.DB.prepare(
      `UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ? AND uses_count < max_uses`
    ).bind(code).run();

    await env.DB.prepare(
      `INSERT INTO promo_uses (code, user_id, used_at) VALUES (?, ?, ?)`
    ).bind(code, userId, now).run();

    return json({ ok: true, reward_snow: promo.reward_snow, code });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/lootbox/open" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const body = await request.json();
    const sessionUserId = await getUserIdFromRequest(request);
    if (!sessionUserId || sessionUserId !== Number(body.user_id)) {
      return json({ error: "Unauthorized" }, 401);
    }
    const userId = sessionUserId;
    const boxType = String(body.box_type || "").trim();

    if (!await checkRateLimit(env, userId, 'lootbox', 10)) {
      return json({ error: "Too many requests" }, 429);
    }

    const BOXES = {
      snow: {
        cost: 200,
        costType: 'snow',
        prizes: [
          { label: '25 ❄️',   type: 'snow',    amount: 25,    weight: 300 },
          { label: '50 ❄️',   type: 'snow',    amount: 50,    weight: 250 },
          { label: '100 ❄️',  type: 'snow',    amount: 100,   weight: 200 },
          { label: '25 ☃️',   type: 'snowman', amount: 25,    weight: 100 },
          { label: '50 ☃️',   type: 'snowman', amount: 50,    weight: 80  },
          { label: '100 ☃️',  type: 'snowman', amount: 100,   weight: 50  },
          { label: '500 ☃️',  type: 'snowman', amount: 500,   weight: 12  },
          { label: '750 ☃️',  type: 'snowman', amount: 750,   weight: 6   },
          { label: '1000 ❄️', type: 'snow',    amount: 1000,  weight: 3   },
          { label: '500 ❄️',  type: 'snow',    amount: 500,   weight: 3   },
          { label: '1000 ☃️', type: 'snowman', amount: 1000,  weight: 2   },
          { label: '0.2 TON', type: 'ton',     amount: 0.2,   weight: 0.00000001 },
        ]
      },
      ton: {
        cost: 1,
        costType: 'ton',
        prizes: [
  { label: '250 ❄️',  type: 'snow',    amount: 250,   weight: 100 },
  { label: '500 ❄️',  type: 'snow',    amount: 500,   weight: 100 },
  { label: '1000 ❄️',  type: 'snow',    amount: 1000,   weight: 100 },
  { label: '2000 ❄️',  type: 'snow',    amount: 2000,   weight: 100 },
  { label: '250 ☃️',  type: 'snowman', amount: 250,   weight: 100 },
  { label: '500 ☃️',  type: 'snowman', amount: 500,   weight: 100 },
  { label: '1000 ☃️', type: 'snowman', amount: 1000,  weight: 100 },
  { label: '1500 ☃️', type: 'snowman', amount: 1500,  weight: 100 },
  { label: '2000 ☃️', type: 'snowman', amount: 2000,  weight: 100 },
  { label: '0.5 TON',  type: 'ton',     amount: 0.5,   weight: 0.2 },
  { label: '1 TON',    type: 'ton',     amount: 1,     weight: 0.1 },
  { label: '3 TON',    type: 'ton',     amount: 3,     weight: 0.000001 },
]
      }
    };

    const box = BOXES[boxType];
    if (!box) return json({ error: "Invalid box type" }, 400);

    const user = await getUser(env, userId);
if (!user) return json({ error: "User not found" }, 404);

const now = Date.now();

const totalWeight = box.prizes.reduce((sum, p) => sum + p.weight, 0);

let rand = Math.random() * totalWeight;
let prizeIndex = box.prizes.length - 1;
let prize = box.prizes[prizeIndex];

for (let i = 0; i < box.prizes.length; i++) {
  rand -= box.prizes[i].weight;
  if (rand <= 0) {
    prizeIndex = i;
    prize = box.prizes[i];
    break;
  }
}

const deductResult = await env.DB.prepare(
  box.costType === 'snow'
    ? `UPDATE users
       SET snow_balance = snow_balance - ?, updated_at = ?
       WHERE user_id = ? AND snow_balance >= ?`
    : `UPDATE users
       SET ton_balance = ton_balance - ?, updated_at = ?
       WHERE user_id = ? AND ton_balance >= ?`
).bind(box.cost, now, userId, box.cost).run();

if (deductResult.meta.changes === 0) {
  return json({ error: box.costType === 'snow' ? "Not enough Snow" : "Not enough TON" }, 400);
}

if (prize.type === 'snow') {
  await env.DB.prepare(
    `UPDATE users
     SET snow_balance = snow_balance + ?, updated_at = ?
     WHERE user_id = ?`
  ).bind(prize.amount, now, userId).run();
} else if (prize.type === 'snowman') {
  await env.DB.prepare(
    `UPDATE users
     SET snowman_count = snowman_count + ?, last_mined_at = ?, updated_at = ?
     WHERE user_id = ?`
  ).bind(prize.amount, now, now, userId).run();
} else if (prize.type === 'ton') {
  await env.DB.prepare(
    `UPDATE users
     SET ton_balance = ton_balance + ?, updated_at = ?
     WHERE user_id = ?`
  ).bind(prize.amount, now, userId).run();
}

await env.DB.prepare(
  `INSERT INTO lootbox_history (user_id, box_type, reward_type, reward_amount, created_at)
   VALUES (?, ?, ?, ?, ?)`
).bind(userId, boxType, prize.type, prize.amount, now).run();

return json({
  ok: true,
  prize,
  prize_index: prizeIndex,
  prizes: box.prizes
});
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}
    
if (url.pathname === "/api/leaderboard" && request.method === "GET") {
  try {
    const now = Date.now();
    const userId = Number(url.searchParams.get("user_id"));

    // إذا الكاش موجود وأقل من 10 دقائق
    if (leaderboardCache && (now - leaderboardCacheTime) < LEADERBOARD_CACHE_MS) {
      
      // نسخة للمستخدم الحالي فقط (استعلامات خفيفة)
      const result = {
        referrals: { ...leaderboardCache.referrals, user_rank: 0, user_value: 0 },
        snowmen:   { ...leaderboardCache.snowmen,   user_rank: 0, user_value: 0 }
      };

      if (userId) {
        const [refValueRow, snowValueRow] = await Promise.all([
          env.DB.prepare(`SELECT COUNT(*) as ref_count FROM users WHERE referred_by = ?`).bind(userId).first(),
          env.DB.prepare(`SELECT snowman_count FROM users WHERE user_id = ?`).bind(userId).first()
        ]);

        const refValue  = Number(refValueRow?.ref_count || 0);
        const snowValue = Number(snowValueRow?.snowman_count || 0);

        const [refRankRow, snowRankRow] = await Promise.all([
          env.DB.prepare(`SELECT COUNT(*) as rank FROM users WHERE user_id IN (
            SELECT u.user_id FROM users u
            LEFT JOIN users r ON r.referred_by = u.user_id
            GROUP BY u.user_id
            HAVING COUNT(r.user_id) > ?
          )`).bind(refValue).first(),
          env.DB.prepare(`SELECT COUNT(*) + 1 as rank FROM users WHERE snowman_count > ?`).bind(snowValue).first()
        ]);

        result.referrals.user_rank  = Number(refRankRow?.rank  || 0) + 1;
        result.referrals.user_value = refValue;
        result.snowmen.user_rank    = Number(snowRankRow?.rank  || 1);
        result.snowmen.user_value   = snowValue;
      }

      return json(result);
    }

    // بناء الكاش الجديد (يحدث مرة كل 10 دقائق فقط)
    const [refTop, snowTop] = await Promise.all([
      env.DB.prepare(`
        SELECT u.user_id, u.username, u.display_name,
               COUNT(r.user_id) as ref_count
        FROM users u
        LEFT JOIN users r ON r.referred_by = u.user_id
        GROUP BY u.user_id
        ORDER BY ref_count DESC
        LIMIT 20
      `).all(),
      env.DB.prepare(`
        SELECT user_id, username, display_name, snowman_count
        FROM users
        ORDER BY snowman_count DESC
        LIMIT 20
      `).all()
    ]);

    leaderboardCache = {
      referrals: { top: refTop.results  || [], user_rank: 0, user_value: 0 },
      snowmen:   { top: snowTop.results || [], user_rank: 0, user_value: 0 }
    };
    leaderboardCacheTime = now;

    // حساب ترتيب المستخدم الحالي
    const result = {
      referrals: { ...leaderboardCache.referrals },
      snowmen:   { ...leaderboardCache.snowmen }
    };

    if (userId) {
      const [refValueRow, snowValueRow] = await Promise.all([
        env.DB.prepare(`SELECT COUNT(*) as ref_count FROM users WHERE referred_by = ?`).bind(userId).first(),
        env.DB.prepare(`SELECT snowman_count FROM users WHERE user_id = ?`).bind(userId).first()
      ]);

      const refValue  = Number(refValueRow?.ref_count || 0);
      const snowValue = Number(snowValueRow?.snowman_count || 0);

      const [refRankRow, snowRankRow] = await Promise.all([
        env.DB.prepare(`SELECT COUNT(*) as rank FROM users WHERE user_id IN (
          SELECT u.user_id FROM users u
          LEFT JOIN users r ON r.referred_by = u.user_id
          GROUP BY u.user_id
          HAVING COUNT(r.user_id) > ?
        )`).bind(refValue).first(),
        env.DB.prepare(`SELECT COUNT(*) + 1 as rank FROM users WHERE snowman_count > ?`).bind(snowValue).first()
      ]);

      result.referrals.user_rank  = Number(refRankRow?.rank  || 0) + 1;
      result.referrals.user_value = refValue;
      result.snowmen.user_rank    = Number(snowRankRow?.rank  || 1);
      result.snowmen.user_value   = snowValue;
    }

    return json(result);
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}
    
if (url.pathname === "/api/pvp/history" && request.method === "GET") {
  try {
    const rounds = await env.DB.prepare(
      `SELECT r.*, u.username, u.display_name
       FROM pvp_rounds r
       LEFT JOIN users u ON u.user_id = r.winner_id
       WHERE r.status = 'finished'
       ORDER BY r.id DESC LIMIT 20`
    ).all();
    return json({ rounds: rounds.results || [] });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}
    
if (url.pathname === "/api/market/listings" && request.method === "GET") {
  try {
    const userId = Number(url.searchParams.get("user_id") || 0);
    const listings = await env.DB.prepare(
      `SELECT m.*, u.display_name, u.username FROM market_listings m
       LEFT JOIN users u ON u.user_id = m.seller_id
       WHERE m.status = 'active'
       ORDER BY m.created_at DESC LIMIT 50`
    ).all();
    const yours = listings.results?.filter(l => l.seller_id === userId) || [];
    const all = listings.results?.filter(l => l.seller_id !== userId) || [];
    return json({ all, yours });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/market/create" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const body = await request.json();
    const sessionUserId = await getUserIdFromRequest(request);
    if (!sessionUserId || sessionUserId !== Number(body.user_id)) {
        return json({ error: "Unauthorized" }, 401);
    }
    const userId = Number(body.user_id);
    const snowAmount = Number(body.snow_amount);
    const pricePerSnow = Number(body.price_per_snow);

    if (!userId) return json({ error: "Missing user_id" }, 400);
    if (snowAmount < 100) return json({ error: "Minimum 100 Snow" }, 400);
    if (pricePerSnow < 0.00005) return json({ error: "Minimum price 0.00005 TON per Snow" }, 400);

    const user = await getUser(env, userId);
    if (!user) return json({ error: "User not found" }, 404);

    const activeCount = await env.DB.prepare(
      `SELECT COUNT(*) as cnt FROM market_listings WHERE seller_id = ? AND status = 'active'`
    ).bind(userId).first();
if ((activeCount?.cnt || 0) >= 2) return json({ error: "Max 2 active orders allowed" }, 400);

    const fee = snowAmount * 0.05;
    const totalSnowCost = snowAmount + fee;
    if (Number(user.snow_balance) < totalSnowCost) return json({ error: "Not enough Snow" }, 400);

    const totalPrice = parseFloat((snowAmount * pricePerSnow).toFixed(6));
    const now = Date.now();

    const createResult = await env.DB.prepare(
      `UPDATE users SET snow_balance = snow_balance - ?, updated_at = ? WHERE user_id = ? AND snow_balance >= ?`
    ).bind(totalSnowCost, now, userId, totalSnowCost).run();

    if (createResult.meta.changes === 0) return json({ error: "Not enough Snow" }, 400);

    await env.DB.prepare(
      `INSERT INTO market_listings (seller_id, snow_amount, price_per_snow, total_price, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, 'active', ?, ?)`
    ).bind(userId, snowAmount, pricePerSnow, totalPrice, now, now).run();

    return json({ ok: true, snow_amount: snowAmount, fee, total_price: totalPrice });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/market/buy" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);

  try {
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const userId = auth.userId;
    const listingId = Number(body.listing_id);

    if (!await checkRateLimit(env, userId, "market_buy", 5)) {
      return json({ error: "Too many requests" }, 429);
    }

    const listing = await env.DB.prepare(
      `SELECT * FROM market_listings WHERE id = ? AND status = 'active'`
    ).bind(listingId).first();

    if (!listing) return json({ error: "Listing not found or already sold" }, 404);
    if (listing.seller_id === userId) return json({ error: "Cannot buy your own listing" }, 400);

    const now = Date.now();

    const deductResult = await env.DB.prepare(
      `UPDATE users
       SET ton_balance = ton_balance - ?, updated_at = ?
       WHERE user_id = ? AND ton_balance >= ?`
    ).bind(listing.total_price, now, userId, listing.total_price).run();

    if (deductResult.meta.changes === 0) return json({ error: "Not enough TON" }, 400);

    const soldResult = await env.DB.prepare(
      `UPDATE market_listings
       SET status = 'sold', updated_at = ?
       WHERE id = ? AND status = 'active'`
    ).bind(now, listingId).run();

    if (soldResult.meta.changes === 0) {
      return json({ error: "Listing already sold" }, 409);
    }

    await env.DB.prepare(
      `UPDATE users SET snow_balance = snow_balance + ?, updated_at = ? WHERE user_id = ?`
    ).bind(listing.snow_amount, now, userId).run();

    await env.DB.prepare(
      `UPDATE users SET ton_balance = ton_balance + ?, updated_at = ? WHERE user_id = ?`
    ).bind(listing.total_price, now, listing.seller_id).run();

    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: listing.seller_id,
        text: `Your order of ${listing.snow_amount} Snow has been sold for ${listing.total_price} TON! 🎉`
      })
    });

    return json({ ok: true });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/market/cancel" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const body = await request.json();
    const sessionUserId = await getUserIdFromRequest(request);
    if (!sessionUserId || sessionUserId !== Number(body.user_id)) {
        return json({ error: "Unauthorized" }, 401);
    }
    const userId = sessionUserId;
    const listingId = Number(body.listing_id);

    const listing = await env.DB.prepare(
      `SELECT * FROM market_listings WHERE id = ? AND status = 'active' AND seller_id = ?`
    ).bind(listingId, userId).first();
    if (!listing) return json({ error: "Listing not found" }, 404);

    const CANCEL_FEE = 20;
    const refundSnow = listing.snow_amount - CANCEL_FEE;
    const now = Date.now();

    await env.DB.prepare(
      `UPDATE market_listings SET status = 'cancelled', updated_at = ? WHERE id = ?`
    ).bind(now, listingId).run();

    await env.DB.prepare(
      `UPDATE users SET snow_balance = snow_balance + ?, updated_at = ? WHERE user_id = ?`
    ).bind(Math.max(0, refundSnow), now, userId).run();

    return json({ ok: true, refund: Math.max(0, refundSnow), fee: CANCEL_FEE });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/withdraw" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);

  try {
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const userId = auth.userId;
    const amount = Number(body.amount);
    const tonAddress = String(body.ton_address || "").trim();

    if (!isValidTonAddress(tonAddress)) return json({ error: "Invalid TON address" }, 400);
    if (!userId || !tonAddress) return json({ error: "Missing params" }, 400);
    if (amount < 0.2) return json({ error: "Minimum withdrawal is 0.2 TON" }, 400);

    if (!await checkRateLimit(env, userId, "withdraw", 3)) {
      return json({ error: "Too many requests" }, 429);
    }

    const user = await getUser(env, userId);
    if (!user) return json({ error: "User not found" }, 404);

    const fee = parseFloat((amount * 0.05).toFixed(4));
    const netAmount = parseFloat((amount - fee).toFixed(4));
    const now = Date.now();

    const withdrawResult = await env.DB.prepare(
      `UPDATE users
       SET ton_balance = ton_balance - ?, updated_at = ?
       WHERE user_id = ? AND ton_balance >= ?`
    ).bind(amount, now, userId, amount).run();

    if (withdrawResult.meta.changes === 0) return json({ error: "Not enough TON balance" }, 400);

    const insertResult = await env.DB.prepare(
      `INSERT INTO withdrawals (user_id, amount, fee, net_amount, ton_address, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`
    ).bind(userId, amount, fee, netAmount, tonAddress, now, now).run();

    const withdrawId = insertResult.meta.last_row_id;

    const msgText = `✅ Withdrawal Pending\n\n👤 User: ${user.display_name || user.username || userId}\n💎 Net Amount: ${netAmount} TON\n🏦 Status: Pending\nTon Address: ${tonAddress}\n\n☃️ SnowManBot — Play & Earn TON\n👉 @Snow0ManBot`;

    const keyboard = {
      inline_keyboard: [[
        { text: "✅ Approve", callback_data: `approve_${withdrawId}` },
        { text: "❌ Reject", callback_data: `reject_${withdrawId}` }
      ]]
    };

    const msgRes = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: env.ADMIN_CHANNEL_ID,
        text: msgText,
        reply_markup: keyboard
      })
    });

    const msgData = await msgRes.json();
    const messageId = msgData.result?.message_id;

    if (messageId) {
      await env.DB.prepare(
        `UPDATE withdrawals SET message_id = ? WHERE id = ?`
      ).bind(messageId, withdrawId).run();
    }

    return json({ ok: true, withdraw_id: withdrawId, fee, net_amount: netAmount });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/withdraw/history" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const sessionUserId = await getUserIdFromRequest(request);
if (!sessionUserId) return json({ error: "Unauthorized" }, 401);
const userId = sessionUserId;
    if (!userId) return json({ error: "Missing user_id" }, 400);

    if (!await checkRateLimit(env, userId, 'withdraw_history', 10)) {
    return json({ error: "Too many requests" }, 429);
}

    const withdrawals = await env.DB.prepare(
      `SELECT * FROM withdrawals WHERE user_id = ? ORDER BY created_at DESC LIMIT 20`
    ).bind(userId).all();

    const deposits = await env.DB.prepare(
      `SELECT * FROM ton_deposits WHERE user_id = ? ORDERif (url.pathname === "/api/boost/status" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);
  try {
    const sessionUserId = await getUserIdFromRequest(request);
    if (!sessionUserId) return json({ error: "Unauthorized" }, 401);
    const userId = sessionUserId;
    if (!userId) return json({ error: "Missing user_id" }, 400);
    const user = await getUser(env, userId);
    if (!user) return json({ error: "User not found" }, 404);

    const computed = computeMiningState(user, Date.now());
    return json({
      boost: Number(user.mining_boost || 1),
      speed_per_hour: computed.speedPerHour,
      ton_balance: Number(user.ton_balance || 0)
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/tasks/create" && request.method === "POST") {
  try {
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const result = await handleTaskCreate(env, auth.userId, body, "add_task");
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 400);
  }
}

if (url.pathname === "/api/tasks/list" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);

  const userId = Number(url.searchParams.get("user_id"));
  const tasks = await getOnlineTasks(env);

  let completedIds = [];
  if (userId) {
    const completions = await env.DB.prepare("SELECT task_id FROM task_completions WHERE user_id = ?")
      .bind(userId)
      .all();

    completedIds = (completions.results || []).map((r) => r.task_id);
  }

  return json({ tasks: tasks.results || [], completed_ids: completedIds });
}

if (url.pathname === "/api/tasks/history" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
  const userId = Number(url.searchParams.get("user_id"));
  if (!userId || Number.isNaN(userId)) {
    return json({ error: "Invalid user_id" }, 400);
  }

  const tasks = await getTasksByCreator(env, userId);
  return json({ tasks: tasks.results || [] });
}

if (url.pathname === "/api/tasks/complete" && request.method === "POST") {
  try {
    const body = await request.json();
    const auth = await requireSessionUser(request, env, body.user_id);
    if (!auth.ok) return json({ error: auth.error }, 401);

    const result = await handleTaskComplete(env, auth.userId, body);
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 400);
  }
}
    
if (url.pathname === "/api/hatch" && request.method === "POST") {
  const isValid = await verifyTelegramAuth(request, env);
if (!isValid) return json({ error: "Unauthorized" }, 401);
      try {
        const body = await request.json();
        const sessionUserId = await getUserIdFromRequest(request);
        if (!sessionUserId || sessionUserId !== Number(body.user_id)) {
            return json({ error: "Unauthorized" }, 401);
        }
        const userId = sessionUserId;
        const baseAmount = Math.floor(Number(body.amount));
        const username = body.username || null;
        const displayName = body.display_name || null;

        if (!userId || isNaN(userId) || userId <= 0) {
          return json({ error: "Invalid user_id." }, 400);
        }

if (!await checkRateLimit(env, userId, 'hatch', 5)) {
    return json({ error: "Too many requests" }, 429);
}
        
        if (!Number.isFinite(baseAmount) || baseAmount < 100) {
          return json({ error: "Minimum hatch amount is 100." }, 400);
        }

        const settled = await settleUserMining(env, userId, username, displayName);
        const user = settled.user;

        const fee = Math.ceil(baseAmount * 0.10);
        const total = baseAmount + fee;
        const now = Date.now();

const result = await env.DB
  .prepare(
    `UPDATE users
     SET snow_balance = snow_balance - ?,
         snowman_count = snowman_count + ?,
         updated_at = ?
     WHERE user_id = ? AND snow_balance >= ?`
  )
  .bind(total, baseAmount, now, userId, total)
  .run();

if (result.meta.changes === 0) {
  return json({ error: "Not enough Snow." }, 400);
}

        const hatchUser = await getUser(env, userId);
        if (hatchUser?.referred_by) {
          const snowBonus = baseAmount * 0.05;
          await env.DB.prepare(
            `UPDATE users SET snow_balance = snow_balance + ?, updated_at = ? WHERE user_id = ?`
          ).bind(snowBonus, now, hatchUser.referred_by).run();
        }

        const updatedUser = await getUser(env, userId);

        return json({
          ok: true,
          user: {
            user_id: Number(updatedUser.user_id),
            username: updatedUser.username || null,
            display_name: updatedUser.display_name || null,
            snow_balance: Number(updatedUser.snow_balance || 0),
            snowman_count: Number(updatedUser.snowman_count || 0),
            mining_boost: Number(updatedUser.mining_boost || 1),
            last_mined_at: Number(updatedUser.last_mined_at || 0),
            updated_at: Number(updatedUser.updated_at || 0)
          },
          hatch: {
            amount: baseAmount,
            fee,
            total
          }
        });
      } catch (error) {
        return json({ error: error.message }, 500);
      }
    }
  

if (url.pathname === "/api/bootstrap" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);

  try {
    await ensureSchema(env);

    const sessionUserId = await getUserIdFromRequest(request);
    const userIdParam = url.searchParams.get("user_id");
    const userId = Number(userIdParam);

    if (!userIdParam || Number.isNaN(userId) || userId <= 0) {
      return json({ error: "Invalid user_id. Please provide a numeric ID." }, 400);
    }

    if (!sessionUserId || sessionUserId !== userId) {
      return json({ error: "Unauthorized" }, 401);
    }

    const username = url.searchParams.get("username");
    const displayName = url.searchParams.get("display_name");
    const settled = await settleUserMining(env, userId, username, displayName);

    return json({
      server_time: Date.now(),
      total_snowmen: await getTotalSnowmen(env),
      user: settled.user
    });
  } catch (e) {
    return json({ error: e.message }, 500);
  }
}

if (url.pathname === "/api/me" && request.method === "GET") {
  const isValid = await verifyTelegramAuth(request, env);
  if (!isValid) return json({ error: "Unauthorized" }, 401);

  const sessionUserId = await getUserIdFromRequest(request);
  const userIdParam = url.searchParams.get("user_id");
  const userId = Number(userIdParam);

  if (!userIdParam || isNaN(userId) || userId <= 0) {
    return json({ error: "Invalid user_id. Please provide a numeric ID." }, 400);
  }
  
  if (!sessionUserId || sessionUserId !== userId) {
    return json({ error: "Unauthorized" }, 401);
  }

  if (!await checkRateLimit(env, userId, "settle_mining", 5)) {
    return json({ error: "Too many requests" }, 429);
  }

  const username = url.searchParams.get("username");
  const displayName = url.searchParams.get("display_name");

  try {
    const result = await settleUserMining(env, userId, username, displayName);
    result.total_snowmen = await getTotalSnowmen(env);
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 500);
  }
}
    
return env.ASSETS.fetch(request);
  }
};
