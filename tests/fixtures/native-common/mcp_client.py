"""Exercise the native endpoint through the official, locked MCP client."""
import asyncio
import json
import sys

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client


async def main():
    config = json.load(sys.stdin)
    async with httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {config['token']}"},
        timeout=httpx2.Timeout(30.0),
        trust_env=False,
    ) as http:
        async with Client(streamable_http_client(config["url"], http_client=http)) as client:
            assert client.protocol_version == "2026-07-28"
            assert client.server_info.name == "moria"
            page = await client.list_tools()
            tools = list(page.tools)
            while page.next_cursor:
                page = await client.list_tools(cursor=page.next_cursor)
                tools.extend(page.tools)
            assert len(tools) == 103
            result = await client.call_tool(f"query_{config['target']}", {"query": {}})
            assert not result.is_error
            assert len(result.structured_content["objects"]) == 1
            assert "secret" not in result.structured_content["objects"][0]
            resource = await client.read_resource(
                f"moria://resource/ObjectType/{config['target']}"
            )
            assert json.loads(resource.contents[0].text)["resource"] == config["target"]
            print(json.dumps({"protocolVersion": client.protocol_version, "tools": len(tools), "protectedRows": 1}))


asyncio.run(main())