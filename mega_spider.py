import asyncio
import gc
import hashlib
import logging
import math
import struct
from io import BytesIO

import bencoder
import six

from mega import Maga, random_node_id

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
)
BLOCK_SIZE = 16.0 * 1024
bt_header = b'\x13BitTorrent protocol'
ext_bytes = b'\x00\x00\x00\x00\x00\x10\x00\x01'
HANDSHAKE_PREFIX = bt_header + ext_bytes
# 握手确定 bep_0009 获取元数据扩展协议
BT_MSG_ID = b'\x14'
EXT_HANDSHAKE_ID = b'\x00'


class MetaFetcher:
    def __init__(self, redis, infohash, address):
        self.redis = redis
        self.infohash: bytes = infohash
        self.address = address
        self.reader, self.writer = None, None

    async def init(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(*self.address)
        except Exception:
            pass

    def check_handshake(self, packet):
        if bt_header != packet[:20] and packet[25] & 0x10 != 0:
            logging.error('Packet format error')
            return False
        if packet[28:48] != self.infohash:
            logging.error('Infohash not match')
            return False
        return True

    def send_handshake(self):
        logging.debug("send handshake")
        peer_id = random_node_id()
        packet = HANDSHAKE_PREFIX + self.infohash + peer_id
        self.writer.write(packet)

    def send_message(self, msg):
        msg_len = struct.pack('>I', len(msg))
        self.writer.write(msg_len + msg)

    async def read_message(self):
        msg_len = struct.unpack('>I', await self.reader.read(4))[0]
        if msg_len == 0:
            return b''
        msg = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=3)
        return msg

    def send_ext_handshake(self):
        logging.debug("send ext handshake")
        # msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencoder.bencode({'m': {'ut_metadata': 1}})
        msg = b'\x14\x00d1:md11:ut_metadatai1eee'
        self.send_message(msg)

    def request_metadata(self, ut_metadata, piece):
        logging.debug("request metadata")
        msg = BT_MSG_ID + six.int2byte(ut_metadata) + bencoder.bencode({'msg_type': 0, 'piece': piece})
        self.send_message(msg)

    async def get_metadata(self):
        if not self.writer or not self.reader:
            return
        try:
            # handshake
            self.send_handshake()
            data = await self.reader.readexactly(68)
            if not self.check_handshake(data):
                logging.error('Handshake response error: %s', data)
                return

            # ext handshake
            self.send_ext_handshake()
            metadata = []
            data = BytesIO(await self.read_message())
            if data.read(1) != BT_MSG_ID:
                return
            if data.read(1) != EXT_HANDSHAKE_ID:
                return
            packet = data.read()
            meta_dict = bencoder.bdecode(packet)
            logging.debug('meta_dict: %s', dict(meta_dict))
            ut_metadata, metadata_size = meta_dict[b'm'][b'ut_metadata'], meta_dict.get(b'metadata_size', 0)
            if not metadata_size:
                logging.error('Empty metadata size')
                return
            pieces_count = int(math.ceil(metadata_size / BLOCK_SIZE))
            for piece in range(pieces_count):
                # piece是个控制块，根据控制块下载数据
                self.request_metadata(ut_metadata, piece)
                while True:
                    data = BytesIO(await self.read_message())
                    if data.read(1) != BT_MSG_ID:
                        continue
                    if data.read(1) == EXT_HANDSHAKE_ID:
                        return
                    packet = data.read()
                    piece_dict, index = bencoder.decode_dict(packet, 0)
                    if piece_dict[b'msg_type'] != 1:
                        continue
                    piece = piece_dict[b'piece']
                    piece_len = len(packet) - index
                    if (piece != pieces_count - 1 and piece_len != BLOCK_SIZE) or (
                            piece == pieces_count - 1 and piece_len != metadata_size % BLOCK_SIZE):
                        return
                    metadata.append(packet[index:])
                    break
            metadata = b''.join(metadata)
            sha1_encoder = hashlib.sha1()
            sha1_encoder.update(metadata)
            if self.infohash != sha1_encoder.digest():
                logging.error("Infohash %s not equal to info sha1 %s", self.infohash.hex(), sha1_encoder.hexdigest())
                return
            info = bencoder.bdecode(metadata)
            print(info)
            # 只记录有效元数据
            # if info['size'] != '0' and info['name'] != '':
            #     await self.redis.lpush('metadata', json.dumps(info))
            del metadata
            gc.collect()
        except Exception as e:
            logging.exception(e)
        finally:
            self.close()  # 确保关闭socket

    def close(self):
        self.writer.close()


class Crawler(Maga):
    async def init(self):
        # self.redis = await aioredis.create_redis_pool('redis://localhost')
        self.redis = None
        pass

    async def close(self):
        self.redis.close()

    async def handle_get_peers(self, infohash, addr):
        # await self.redis.lpush("infohash", infohash)
        # logging.info(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # await self.redis.lpush("infohash", infohash)
        logging.info(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")
        fetcher = MetaFetcher(self.redis, bytes.fromhex(infohash), peer_addr)
        await fetcher.init()
        # asyncio.ensure_future(fetcher.get_metadata())
        await fetcher.get_metadata()


if __name__ == '__main__':
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except:
        pass
    crawler = Crawler()
    # Set port to 0 will use a random available port
    crawler.run(port=0)
