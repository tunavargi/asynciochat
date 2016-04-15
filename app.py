import asyncio
import json

import aiohttp_jinja2 as aiohttp_jinja2
import jinja2

import rethinkdb as r
from aiohttp import web
from datetime import datetime



def json_serial(obj):
    if isinstance(obj, datetime):
        serial = obj.ctime()
        return serial
    raise TypeError ("Type not serializable")


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    # messages = await r.table("messages").filter(lambda message: message["message"].match("totoro")).changes().run(connection)
    # messages = await r.table("messages").filter(lambda message: message["username"].match("tuna")).changes().run(connection)
    # messages = await r.table("messages").filter({"username": "tuna"}).changes().run(connection)
    messages = await r.table("messages").changes().run(connection)
    while await messages.fetch_next():
        message = await messages.next()
        message = json.dumps(message.get("new_val"), default=json_serial)
        ws.send_str(message)
    return ws


async def post_message_handler(request):
    data = await request.json()
    if data.get('message'):
        data['time'] = r.now()
        await r.table('messages').insert(data).run(connection)
        return web.Response()


async def old_messages_handler(request):
    old_messages = await r.table("messages").order_by(r.desc('time')).limit(5).run(connection)

    # old_messages = []
    # data = await r.table("messages").limit(5).run(connection)
    # while await data.fetch_next():
    #     message = await data.next()
    #     old_messages.append(message)
    return web.Response(text=json.dumps(old_messages, default=json_serial))

async def prepare():
    global connection
    r.set_loop_type('asyncio')
    connection = await r.connect(host="localhost", port=28015)

@aiohttp_jinja2.template('index.html')
async def index(request):
    return {}

if __name__ == '__main__':
    app = web.Application()
    asyncio.get_event_loop().run_until_complete(prepare())
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('templates'))
    app.router.add_route('POST', '/messages', post_message_handler)
    app.router.add_route('GET', '/messages', old_messages_handler)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/echo', websocket_handler)
    web.run_app(app, port=5000)