Topic Pool MCP

A small external-topic buffer based on the supplied Topic Pool / 实事话题池链路 design:

sources -> Scout -> Filter -> short-lived Topic Pool -> MCP clients -> agent decides whether to follow / discuss / ignore

The server is intentionally MCP-first. ChatGPT, IO resident, Claude, or any other MCP client can share the same pool while keeping separate per-consumer seen / consumed / ignored state.

What it exposes

• topic_pool — list current topics
• topic_pick — pick one unseen topic and mark it seen
• topic_get — expand one topic
• topic_consume — mark used for one consumer
• topic_ignore — ignore for one consumer
• topic_search — search the local buffer (not the live web)
• topic_stats — pool/scout status
• scout_now — run Scout immediately
• interest_get / interest_update — soft interest hints per consumer
• topic_dead — globally remove a broken/duplicate topic

MCP endpoint: /mcp
Health endpoint: /health

Data and TTL

SQLite stores topics in DATA_DIR/topic_pool.sqlite3 by default. Mount a persistent volume at DATA_DIR in Zeabur.

Global topic states: open / expired / dead.
Per-consumer states: seen / consumed / ignored.

This means chatgpt can consume a topic without hiding it from io, and vice versa.

Scout sources

The first version supports:

• Google News RSS — broad current affairs
• Hacker News — software/AI/tech discussion
• arXiv — recent AI/technology/science papers
• GitHub Search API — new repositories gaining attention

Scout runs every SCOUT_INTERVAL_MINUTES (default 360 = 6 hours), picks a few configured categories, deduplicates, filters, and inserts at most MAX_TOPICS_PER_RUN topics.

Filter modes

No API key

Uses a deterministic heuristic filter. It works immediately and costs nothing.

Optional LLM filter (DeepSeek supported)

If you already have a DeepSeek API key, use:

```env
LLM_PROVIDER=deepseek
DEEPSEEK_API_KEY=...
LLM_BASE_URL=https://api.deepseek.com
FILTER_MODEL=deepseek-v4-flash
LLM_FILTER_ENABLED=true
```

The implementation uses the OpenAI Python SDK against DeepSeek’s OpenAI-compatible Chat Completions endpoint. For filtering, DeepSeek is put in non-thinking JSON mode so the call stays cheap and reliably parseable.

You can also point the same code at another OpenAI-compatible provider with LLM_API_KEY, LLM_BASE_URL, and FILTER_MODEL. OPENAI_API_KEY remains accepted as a compatibility shortcut.

The model only receives candidate title/summary/source metadata. Candidate text is treated as untrusted data, not instructions. If the model call fails, the server automatically falls back to the heuristic filter.

Zeabur deployment

1. Upload all files in this folder to one GitHub repository.
2. Create a Zeabur service from that repository. The included Dockerfile is enough; no custom start command is required.
3. Add a persistent Volume and mount it at /data/topic-pool.
4. Add environment variable:

```env
DATA_DIR=/data/topic-pool
```

5. Optional but recommended:

```env
GITHUB_TOKEN=...
```

6. Optional LLM filter variables as above.
7. If you want a shared secret, generate a long random token and set:

```env
MCP_AUTH_TOKEN=...
```

8. After deploy, verify:

https://YOUR_DOMAIN/health

It should return {"status":"ok", ...}.

Your MCP URL is:

https://YOUR_DOMAIN/mcp

Transport security

The official MCP Python SDK v2 protects localhost deployments with a Host allowlist by default. Zeabur already sits behind a reverse proxy, so this project defaults to:

```env
MCP_DISABLE_DNS_REBINDING_PROTECTION=true
```

For a stricter deployment after you know the final hostname, switch it off and set:

```env
MCP_DISABLE_DNS_REBINDING_PROTECTION=false
MCP_ALLOWED_HOSTS=YOUR_DOMAIN,YOUR_DOMAIN:*
```

Client identity

Best option: have each client send a header:

```text
X-Topic-Consumer: chatgpt
```

or

```text
X-Topic-Consumer: io
```

If a client cannot set that header, every MCP tool also accepts an optional consumer_id argument. If neither is supplied, it defaults to chatgpt.

ChatGPT connection note

ChatGPT connects to remote MCP servers, so use the public HTTPS /mcp URL. Depending on the ChatGPT plan/workspace and the custom-app UI available to your account, authentication options can differ. If your UI supports static headers, use the shared secret headers described above. If it only offers OAuth or no authentication, leave MCP_AUTH_TOKEN unset for the first connectivity test, then add an OAuth layer later if you need the endpoint private.

IO resident

IO should connect to the same /mcp URL and use consumer identity io. Its heartbeat can call topic_pick(consumer_id="io"); receiving a topic only marks it seen. Call topic_consume only after it was actually used or discussed.

Notes

• Run one worker on the first deployment. SQLite + one embedded scheduler is intentionally simple.
• Do not mount the database inside the Git repository. Use the Zeabur volume.
• topic_search only searches already-collected material. scout_now is what goes out to refresh the pool.
• The first version does not scrape article bodies. It stores source-provided title/summary/metadata and the original source URL, reducing fragility and prompt-injection exposure.
