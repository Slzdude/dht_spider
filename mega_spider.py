import asyncio
import gc
import logging
import math
import struct

import aioredis
import bencoder
import six

from mega import Maga, random_node_id

logging.basicConfig(level=logging.INFO)

BT_PROTOCOL = 'BitTorrent protocol'

# 握手确定 bep_0009 获取元数据扩展协议
BT_MSG_ID = 20
EXT_HANDSHAKE_ID = 0


class MetaFetcher:
    def __init__(self, redis, infohash, address):
        self.redis = redis
        self.infohash = infohash
        self.address = address
        self.reader, self.writer = await asyncio.open_connection(*address)

    def check_handshake(self, packet):
        try:
            bt_header_len, packet = ord(packet[:1]), packet[1:]
            if bt_header_len != len(BT_PROTOCOL):
                return False
        except TypeError:
            return False

        bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
        if bt_header != BT_PROTOCOL:
            return False

        packet = packet[8:]
        infohash = packet[:20]
        if infohash != self.infohash:
            return False

        return True

    async def send_handshake(self):
        bt_header = b'\x13BitTorrent protocol'
        ext_bytes = b'\x00\x00\x00\x00\x00\x10\x00\x01'
        peer_id = random_node_id()
        packet = struct.pack('ssss', bt_header, ext_bytes, self.infohash, peer_id)
        await self.writer.write(packet)

    async def send_message(self, msg):
        msg_len = struct.pack('>I', len(msg))
        self.writer.write(msg_len + msg)

    async def read_message(self):
        msg_len = struct.unpack('>I', await self.reader.read(4))[0]
        if msg_len == 0:
            return b''
        msg = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=3)
        return msg

    async def send_ext_handshake(self):
        msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencoder.bencode({'m': {'ut_metadata': 1}})
        await self.send_message(msg)

    async def request_metadata(self, ut_metadata, piece):
        msg = b'\x14' + ut_metadata + bencoder.bencode({'msg_type': 0, 'piece': piece})
        await self.send_message(msg)

    @staticmethod
    def get_ut_metadata(data):
        ut_metadata = 'ut_metadata'
        index = data.index(ut_metadata) + len(ut_metadata) + 1
        return six.int2byte(data[index])

    @staticmethod
    def get_metadata_size(data):
        metadata_size = 'metadata_size'
        start = data.index(metadata_size) + len(metadata_size) + 1
        data = data[start:]
        return int(data[:data.index('e')])

    async def get_metadata(self):
        info = {}
        try:
            # handshake
            await self.send_handshake()
            packet = await self.reader.read(68)
            print(bencoder.bdecode(packet))
            if not self.check_handshake(packet):
                return

            # ext handshake
            await self.send_ext_handshake()
            packet = await self.read_message()
            print(bencoder.bdecode(packet))
            msg_type = (await self.reader.read(1))[0]
            if msg_type != BT_MSG_ID:
                return
            extended_id = (await self.reader.read(1))[0]
            if extended_id != EXT_HANDSHAKE_ID:
                return

            ut_metadata, metadata_size = self.get_ut_metadata(packet), self.get_metadata_size(packet)
            # request each piece of metadata
            metadata = []
            for piece in range(int(math.ceil(metadata_size / (16.0 * 1024)))):  # piece是个控制块，根据控制块下载数据
                await self.request_metadata(ut_metadata, piece)
                try:
                    # Wait for 3 seconds, then raise TimeoutError
                    packet = await asyncio.wait_for(self.reader.read(4096), timeout=3)
                except asyncio.TimeoutError:
                    print("Timeout")
                    return

                metadata.append(packet[packet.index(b'ee') + 2:])
            metadata = b''.join(metadata)
            info = bencoder.bdecode(metadata)
            print(info)
            # 只记录有效元数据
            # if info['size'] != '0' and info['name'] != '':
            #     await self.redis.lpush('metadata', json.dumps(info))
            del metadata
            gc.collect()
        except Exception as e:
            print(e)
        finally:
            self.close()  # 确保关闭socket

    def close(self):
        self.writer.close()


class Crawler(Maga):
    async def init(self):
        # self.redis = await aioredis.create_redis_pool('redis://localhost')
        pass

    async def close(self):
        self.redis.close()

    async def handle_get_peers(self, infohash, addr):
        await self.redis.lpush("infohash", infohash)
        logging.info(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # await self.redis.lpush("infohash", infohash)
        logging.info(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")
        asyncio.create_task(MetaFetcher(self.redis, infohash, peer_addr).get_metadata())


crawler = Crawler()
# Set port to 0 will use a random available port
crawler.run(port=0)
