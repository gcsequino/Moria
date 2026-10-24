import asyncio
import json
import os
import re
from aiohttp import web
from psycopg_pool import AsyncConnectionPool

queries = {
    key: re.sub(r"\$\d+", "%s", value)
    for key, value in json.load(open("benchmarks/ontology/queries.json")).items()
}
pool = AsyncConnectionPool(
    os.environ["BENCH_DSN"],
    min_size=0,
    max_size=20,
    open=False,
    kwargs={"autocommit": True, "prepare_threshold": None},
)


async def one(c, name, args):
    result = await c.execute(queries[name], args)
    row = await result.fetchone()
    return json.loads(row[0]) if row else None


async def handle(request):
    try:
        async with pool.connection() as c:
            locked = began = False
            try:
                if request.path == "/action":
                    await c.execute("SELECT pg_advisory_lock(91011)")
                    locked = True
                    await c.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                    began = True
                auth = await one(c, "auth", [request.headers.get("Authorization", "")])
                if auth is None:
                    return web.json_response({}, status=401)
                region = auth["region"]
                if request.path == "/queue":
                    name = "queue_json" if request.query.get("layout") == "json" else "queue_typed"
                    result = await one(
                        c,
                        name,
                        [
                            region,
                            int(request.query.get("priority", 0)),
                            int(request.query.get("after", 0)),
                        ],
                    )
                elif request.path == "/detail":
                    result = await one(c, "detail", [int(request.query["id"]), region])
                elif request.path == "/action":
                    result = await one(c, "receipt", [request.query["key"]])
                    if result is None:
                        result = await one(
                            c, "action", [int(request.query["id"]), region, request.query["key"]]
                        )
                    await c.execute("COMMIT")
                    began = False
                else:
                    return web.json_response({}, status=404)
                return web.json_response(
                    result if result is not None else {},
                    status=200 if result is not None else 404,
                    dumps=lambda x: json.dumps(x, separators=(",", ":")),
                )
            finally:
                if began:
                    await c.execute("ROLLBACK")
                if locked:
                    await c.execute("SELECT pg_advisory_unlock(91011)")
    except Exception:
        return web.json_response({}, status=500)


async def main():
    await pool.open()
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", int(os.environ["PORT"])).start()
    print("READY", flush=True)
    await asyncio.Event().wait()


asyncio.run(main())
