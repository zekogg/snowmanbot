const WEB_APP_URL = "https://snowmanbot-api.zekobusiness0.workers.dev/";

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
      return new Response(
        JSON.stringify({
          status: "ok",
          message: "SnowmanBot API is running"
        }),
        {
          headers: {
            "Content-Type": "application/json"
          }
        }
      );
    }

    return env.ASSETS.fetch(request);
  }
};
