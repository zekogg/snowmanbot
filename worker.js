const WEB_APP_URL = "https://snowmanbot-api.zekobusiness0.workers.dev/";

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // إعداد CORS للـ API فقط
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    // معالجة طلبات OPTIONS (Preflight)
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // ==============================================
    // 1. مسار Telegram Webhook (القديم)
    // ==============================================
    if (path === "/telegram" && request.method === "POST") {
      const update = await request.json();
      const text = update.message?.text;
      const chatId = update.message?.chat?.id;

      if (text === "/start" && chatId) {
        await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
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

    // ==============================================
    // 2. مسارات API الخاصة بالتعدين (الجديدة)
    // ==============================================
    try {
      if (path === '/api/user' && request.method === 'GET') {
        return await handleGetUser(request, env, corsHeaders);
      } else if (path === '/api/mint' && request.method === 'POST') {
        return await handleMint(request, env, corsHeaders);
      } else if (path === '/api/hatch' && request.method === 'POST') {
        return await handleHatch(request, env, corsHeaders);
      } else if (path === '/api/status') {
        // نقطة النهاية القديمة للفحص
        return new Response(JSON.stringify({ status: "ok", message: "SnowmanBot API is running" }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
    } catch (error) {
      console.error(error);
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // ==============================================
    // 3. تقديم الملفات الثابتة (الواجهة الأمامية)
    // ==============================================
    return env.ASSETS.fetch(request);
  },
};

// ==============================================
// الدوال المساعدة ومنطق التعدين
// ==============================================

async function getUserWithOfflineReward(env, userId) {
  const now = Math.floor(Date.now() / 1000);
  const stmt = env.DB.prepare('SELECT * FROM users WHERE user_id = ?');
  let user = await stmt.bind(userId).first();

  if (!user) {
    const insert = env.DB.prepare(`
      INSERT INTO users (user_id, snowflakes, snowmen, mining_speed, last_update, created_at)
      VALUES (?, 0, 0, 0, ?, ?)
    `);
    await insert.bind(userId, now, now).run();
    user = await stmt.bind(userId).first();
  }

  const lastUpdate = user.last_update;
  const secondsPassed = now - lastUpdate;
  const hoursPassed = secondsPassed / 3600;

  if (hoursPassed > 0 && user.mining_speed > 0) {
    const earned = hoursPassed * user.mining_speed;
    const newSnowflakes = user.snowflakes + earned;

    const update = env.DB.prepare(`
      UPDATE users
      SET snowflakes = ?, last_update = ?
      WHERE user_id = ?
    `);
    await update.bind(newSnowflakes, now, userId).run();

    user.snowflakes = newSnowflakes;
    user.last_update = now;
  }

  return user;
}

async function handleGetUser(request, env, corsHeaders) {
  const url = new URL(request.url);
  const userId = url.searchParams.get('userId');

  if (!userId) {
    return new Response(JSON.stringify({ error: 'Missing userId' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  const user = await getUserWithOfflineReward(env, userId);

  return new Response(JSON.stringify({
    userId: user.user_id,
    snowflakes: user.snowflakes,
    snowmen: user.snowmen,
    miningSpeed: user.mining_speed,
    lastUpdate: user.last_update,
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders },
  });
}

async function handleMint(request, env, corsHeaders) {
  const body = await request.json();
  const { userId, packId } = body;

  if (!userId || !packId) {
    return new Response(JSON.stringify({ error: 'Missing userId or packId' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  const packSnowmen = {
    'starter': 2450,
    'pro': 10500,
    'whale': 175000,
  };

  const snowmenToAdd = packSnowmen[packId];
  if (!snowmenToAdd) {
    return new Response(JSON.stringify({ error: 'Invalid packId' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  let user = await getUserWithOfflineReward(env, userId);
  const now = Math.floor(Date.now() / 1000);

  const newSnowmen = user.snowmen + snowmenToAdd;
  const newMiningSpeed = newSnowmen / 350;

  const update = env.DB.prepare(`
    UPDATE users
    SET snowmen = ?, mining_speed = ?, last_update = ?
    WHERE user_id = ?
  `);
  await update.bind(newSnowmen, newMiningSpeed, now, userId).run();

  user = await env.DB.prepare('SELECT * FROM users WHERE user_id = ?').bind(userId).first();

  return new Response(JSON.stringify({
    userId: user.user_id,
    snowflakes: user.snowflakes,
    snowmen: user.snowmen,
    miningSpeed: user.mining_speed,
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders },
  });
}

async function handleHatch(request, env, corsHeaders) {
  const body = await request.json();
  const { userId, amount } = body;

  if (!userId || !amount || amount < 100) {
    return new Response(JSON.stringify({ error: 'Invalid request. amount must be >= 100' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  let user = await getUserWithOfflineReward(env, userId);
  const now = Math.floor(Date.now() / 1000);

  const requiredSnowflakes = Math.ceil(amount / 0.94);

  if (user.snowflakes < requiredSnowflakes) {
    return new Response(JSON.stringify({ error: 'Insufficient snowflakes' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  const newSnowflakes = user.snowflakes - requiredSnowflakes;
  const newSnowmen = user.snowmen + amount;
  const newMiningSpeed = newSnowmen / 350;

  const update = env.DB.prepare(`
    UPDATE users
    SET snowflakes = ?, snowmen = ?, mining_speed = ?, last_update = ?
    WHERE user_id = ?
  `);
  await update.bind(newSnowflakes, newSnowmen, newMiningSpeed, now, userId).run();

  user = await env.DB.prepare('SELECT * FROM users WHERE user_id = ?').bind(userId).first();

  return new Response(JSON.stringify({
    userId: user.user_id,
    snowflakes: user.snowflakes,
    snowmen: user.snowmen,
    miningSpeed: user.mining_speed,
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders },
  });
}
