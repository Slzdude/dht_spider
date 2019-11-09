import asyncio
import signal

import bencoder

from dht_spider.logger import setup_logger
from dht_spider.utils import random_node_id, proper_infohash, split_nodes

logger = setup_logger("Mega Spider")

TOKEN_LENGTH = 2

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)


class Maga(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=1):
        self.node_id = random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.bootstrap_nodes = bootstrap_nodes
        self.__running = False
        self.interval = interval

    def stop(self):
        self.__running = False
        self.loop.call_later(self.interval, self.loop.stop)

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            await asyncio.sleep(self.interval)
            for node in self.bootstrap_nodes:
                self.find_node(addr=node)

    def run(self, port=6881):
        logger.info("Starting Mega crawler")
        coro = self.loop.create_datagram_endpoint(
            lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(coro)

        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # SIGINT and SIGTERM are not implemented on windows
                pass

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)

        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def datagram_received(self, data, addr):
        try:
            msg = bencoder.bdecode(data)
        except Exception as e:
            return
        try:
            self.handle_message(msg, addr)
        except Exception as e:
            self.send_message(data={
                "t": msg["t"],
                "y": "e",
                "e": [202, "Server Error"]
            }, addr=addr)
            raise e

    def handle_message(self, msg, addr):
        msg_type = msg.get(b"y", b"e")

        if msg_type == b"e":
            return

        if msg_type == b"r":
            return self.handle_response(msg, addr=addr)

        if msg_type == b'q':
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        args = msg[b"r"]
        if b"nodes" in args:
            for node_id, ip, port in split_nodes(args[b"nodes"]):
                self.ping(addr=(ip, port))

    async def handle_query(self, msg, addr):
        args = msg[b"a"]
        node_id = args[b"id"]
        query_type = msg[b"q"]
        if query_type == b"get_peers":
            infohash = args[b"info_hash"]
            infohash = proper_infohash(infohash)
            token = infohash[:TOKEN_LENGTH]
            self.send_message({
                "t": msg[b"t"],
                "y": "r",
                "r": {
                    "id": self.fake_node_id(node_id),
                    "nodes": "",
                    "token": token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == b"announce_peer":
            infohash = proper_infohash(args[b"info_hash"])
            if infohash[:TOKEN_LENGTH] != args[b"token"].decode():
                logger.error("Bad Token: %s %s", infohash, args[b'token'])
                return
            if args.get(b"implied_port", 0):
                port = addr[1]
            else:
                port = args[b"port"]
            if port < 1 or port > 65535:
                logger.error("Bad Port: %d", port)
                return
            tid = msg[b"t"]
            self.send_message({
                "t": tid,
                "y": "r",
                "r": {
                    "id": self.fake_node_id(node_id)
                }
            }, addr=addr)
            await self.handle_announce_peer(infohash, addr, [addr[0], port])
        elif query_type == b"find_node":
            tid = msg[b"t"]
            self.send_message({
                "t": tid,
                "y": "r",
                "r": {
                    "id": self.fake_node_id(node_id),
                    "nodes": ""
                }
            }, addr=addr)
        elif query_type == b"ping":
            self.send_message({
                "t": b"tt",
                "y": "r",
                "r": {
                    "id": self.fake_node_id(node_id)
                }
            }, addr=addr)
        self.find_node(addr=addr, node_id=node_id)

    def ping(self, addr, node_id=None):
        self.send_message({
            "y": "q",
            "t": "pg",
            "q": "ping",
            "a": {
                "id": self.fake_node_id(node_id)
            }
        }, addr=addr)

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.__running = False
        self.transport.close()

    def send_message(self, data, addr):
        data.setdefault("t", b"tt")
        self.transport.sendto(bencoder.bencode(data), addr)

    def fake_node_id(self, node_id=None):
        if node_id:
            return node_id[:-1] + self.node_id[-1:]
        return self.node_id

    def find_node(self, addr, node_id=None, target=None):
        if not target:
            target = random_node_id()
        self.send_message({
            "t": b"fn",
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.fake_node_id(node_id),
                "target": target
            }
        }, addr=addr)

    async def handle_get_peers(self, infohash, addr):
        await self.handler(infohash, addr)

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        await self.handler(infohash, addr)

    async def handler(self, infohash, addr):
        pass
