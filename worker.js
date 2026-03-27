export default {
  async fetch(request, env) {
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
};
