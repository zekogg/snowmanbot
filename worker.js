const WEB_APP_URL = "https://snowmanbot-api.zekobusiness0.workers.dev/";
const HOUR_MS = 60 * 60 * 1000;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

async function ensureSchema(env) {
  await env.DB.exec(`
    CREATE TABLE IF NOT EXISTS users (
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      display_name TEXT,
      snow_balance INTEGER NOT NULL DEFAULT 0,
      snowman_count INTEGER NOT NULL DEFAULT 0,
      mining_boost REAL NOT NULL DEFAULT 1,
      last_mined_at INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL DEFAULT 0
    );
  `);
}

async function getUser(env, userId) {
  return await env.DB
    .prepare(
      `SELECT user_id, username, display_name, snow_balance, snowman_count, mining_boost, last_mined_at, updated_at
       FROM users
       WHERE user_id = ?`
    )
    .bind(userId)
    .first();
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

function computeMiningState(user, now = Date.now()) {
  const snowmanCount = Number(user.snowman_count || 0);
  const baseSpeed = Math.floor(snowmanCount / 350);
  const speedPerHour = baseSpeed;

  const lastMinedAt = Number(user.last_mined_at || now);
  const elapsed = Math.max(0, now - lastMinedAt);
  const fullHours = Math.floor(elapsed / HOUR_MS);
  const earnedNow = fullHours * speedPerHour;

  const nextLastMinedAt = lastMinedAt + fullHours * HOUR_MS;
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
  } else if (!user.last_mined_at) {
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
      next_reward_in_ms: finalComputed.nextRewardInMs
    },
    server_time: now
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/telegram" && request.method === "POST") {
      const update = await request.json();
      const text = update.message?.text;
      const chatId = update.message?.chat?.id;

      if (text === "/start" && chatId) {
        await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            chat_id: chatId,
            text: "Welcome to SnowManBot Empire ☃️",
            reply_markup: {
              inline_keyboard: [[
                {
                  text: "Open",
                  web_app: { url: WEB_APP_URL }
                }
              ]]
            }
          })
        });
      }

      return new Response("ok");
    }

    if (url.pathname === "/api/status") {
      return json({
        status: "ok",
        message: "SnowmanBot API is running"
      });
    }

    if (url.pathname === "/api/me" && request.method === "GET") {
      const userId = Number(url.searchParams.get("user_id"));

      if (!Number.isFinite(userId) || userId <= 0) {
        return json({ error: "user_id is required" }, 400);
      }

      const username = url.searchParams.get("username");
      const displayName = url.searchParams.get("display_name");

      const result = await settleUserMining(env, userId, username, displayName);
      return json(result);
    }

    return env.ASSETS.fetch(request);
  }
};
