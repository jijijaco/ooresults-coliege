# Copyright (C) 2022 Rainer Garus
#
# This file is part of the ooresults Python package, a software to
# compute results of orienteering events.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import asyncio
import threading
import ssl
from typing import Optional

import websockets

from ooresults.websocket_server.streaming import Streaming
from ooresults.websocket_server.websocket_handler import WebSocketHandler
from ooresults.repo.event_type import EventType


class WebSocketServer(threading.Thread):
    def __init__(
        self,
        demo_reader: bool = False,
        import_stream: bool = False,
        host: str = "0.0.0.0",
        port: int = 8081,
        ssl_cert=None,
        ssl_key=None,
    ):
        super().__init__()
        self.daemon = True
        self.demo_reader = demo_reader
        self.import_stream = import_stream
        self.handler: Optional[WebSocketHandler] = None
        self.streaming: Optional[Streaming] = None
        self.host = host
        self.port = port
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.loop = None

    def run(self):
        if self.ssl_cert is None:
            ssl_context = None
        else:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(certfile=self.ssl_cert, keyfile=self.ssl_key)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop=self.loop)

        self.streaming = Streaming(loop=self.loop)
        self.handler = WebSocketHandler(
            demo_reader=self.demo_reader, import_stream=self.import_stream
        )
        start_server = websockets.serve(
            self.handler.handler, self.host, self.port, loop=self.loop, ssl=ssl_context
        )
        self.loop.run_until_complete(start_server)
        if ssl_context is None:
            print(f"ws://{self.host}:{str(self.port)}")
        else:
            print(f"wss://{self.host}:{str(self.port)}")
        self.loop.run_forever()

    async def update_event(self, event: EventType) -> None:
        if self.handler:
            await self.handler.update_event(event=event)
        if self.streaming:
            await self.streaming.update_event(event=event)
