const WEB_APP_URL = "https://snowmanbot-api.zekobusiness0.workers.dev/";
const HOUR_MS = 60 * 60 * 1000;

// إصلاح دالة json لدعم الـ CORS (ضروري لعمل البوت في المتصفح وتليجرام)
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*", // يسمح بالوصول من أي مصدر
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
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
      snow_balance REAL NOT NULL DEFAULT 0,
      snowman_count INTEGER NOT NULL DEFAULT 0,
      mining_boost REAL NOT NULL DEFAULT 1,
      last_mined_at INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL DEFAULT 0
    )
  `);

  await env.DB.exec(`
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
  `);

  await env.DB.exec(`
    CREATE TABLE IF NOT EXISTS task_completions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      completed_at INTEGER NOT NULL,
      UNIQUE(task_id, user_id)
    )
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
  const text = [
    "New task pending review",
    "",
    `Title: ${task.title}`,
    `Link: ${task.link}`,
    `Target users: ${task.target_users}`,
    `Completion limit: ${task.completion_limit}`,
    `Cost: ${task.cost_snow} Snow`,
    `Task ID: ${task.task_id}`
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

async function handleTaskCreate(env, body, sourceType) {
  await ensureSchema(env);

  const userId = Number(body.user_id);
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

async function handleTaskComplete(env, body) {
  await ensureSchema(env);

  const userId = Number(body.user_id);
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
   // القاعدة كما هي: تحتاج 350 لتبدأ التعدين (سرعة 1/ساعة)
  const baseSpeed = snowmanCount / 350;
  const speedPerHour = baseSpeed;

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
      next_reward_in_ms: finalComputed.nextRewardInMs
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
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    if (url.pathname === "/telegram" && request.method === "POST") {
      try {
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
                  { text: "Open", web_app: { url: WEB_APP_URL } }
                ]]
              }
            })
          });
        }
        if (update.callback_query) {
          const data = update.callback_query.data || "";
          const callbackId = update.callback_query.id;
          const [action, taskIdRaw] = data.split(":");
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
              body: JSON.stringify({
                callback_query_id: callbackId,
                text: "Updated"
              })
            });
          } catch (error) {
            await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/answerCallbackQuery`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                callback_query_id: callbackId,
                text: "Failed"
              })
            });
          }
        }
      } catch (e) {}
      return new Response("ok");
    }

    if (url.pathname === "/api/status" && request.method === "GET") {
  return json({ status: "ok", message: "SnowmanBot API is running" });
}

if (url.pathname === "/api/tasks/create" && request.method === "POST") {
  try {
    const body = await request.json();
    const result = await handleTaskCreate(env, body, "add_task");
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 400);
  }
}

if (url.pathname === "/api/tasks/submit-channel" && request.method === "POST") {
  try {
    const body = await request.json();
    const result = await handleTaskCreate(env, body, "submit_channel");
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 400);
  }
}

if (url.pathname === "/api/tasks/list" && request.method === "GET") {
  const tasks = await getOnlineTasks(env);
  return json({ tasks: tasks.results || [] });
}

if (url.pathname === "/api/tasks/history" && request.method === "GET") {
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
    const result = await handleTaskComplete(env, body);
    return json(result);
  } catch (error) {
    return json({ error: error.message }, 400);
  }
}
    
if (url.pathname === "/api/hatch" && request.method === "POST") {
      try {
        const body = await request.json();
        const userId = Number(body.user_id);
        const baseAmount = Math.floor(Number(body.amount));
        const username = body.username || null;
        const displayName = body.display_name || null;

        if (!userId || isNaN(userId) || userId <= 0) {
          return json({ error: "Invalid user_id." }, 400);
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
    
    if (url.pathname === "/api/me" && request.method === "GET") {
      const userIdParam = url.searchParams.get("user_id");
      const userId = Number(userIdParam);

      // التحقق من أن user_id رقم صحيح
      if (!userIdParam || isNaN(userId) || userId <= 0) {
        return json({ error: "Invalid user_id. Please provide a numeric ID." }, 400);
      }

      const username = url.searchParams.get("username");
      const displayName = url.searchParams.get("display_name");

      try {
        const result = await settleUserMining(env, userId, username, displayName);
        return json(result);
      } catch (error) {
        return json({ error: error.message }, 500);
      }
    }

    return env.ASSETS.fetch(request);
  }
};
