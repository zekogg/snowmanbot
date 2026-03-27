export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Endpoint: Get user data
    if (url.pathname === "/get-user") {
      const userId = url.searchParams.get("id");
      const userData = await env.USER_DATA.get(userId);
      return new Response(JSON.stringify({ userId, data: userData }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    // Endpoint: Set/update user data
    if (url.pathname === "/set-user") {
      const { id, resources } = await request.json();
      await env.USER_DATA.put(id, JSON.stringify({ resources }));
      return new Response(JSON.stringify({ status: "ok" }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    // Default response
    return new Response(JSON.stringify({
      status: "ok",
      message: "SnowmanBot API is running"
    }), {
      headers: { "Content-Type": "application/json" }
    });
  }
};
