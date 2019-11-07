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
    def __init__(self, infohash, address):
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
        return BytesIO(msg)

    def send_ext_handshake(self):
        logging.debug("send ext handshake")
        # msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencoder.bencode({'m': {'ut_metadata': 1}})
        msg = b'\x14\x00d1:md11:ut_metadatai1eee'
        self.send_message(msg)

    def request_metadata(self, ut_metadata, piece):
        logging.debug("request metadata")
        msg = BT_MSG_ID + six.int2byte(ut_metadata) + bencoder.bencode({'msg_type': 0, 'piece': piece})
        self.send_message(msg)

    @staticmethod
    def pieces_done(pieces):
        for piece in pieces:
            if piece is None:
                return False
        return True

    @staticmethod
    def check_piece(piece, piece_index, piece_count, metadata_size):
        if piece_index == piece_count - 1:
            return len(piece) == metadata_size % BLOCK_SIZE
        else:
            return len(piece) == BLOCK_SIZE

    async def get_metadata(self):
        if not self.writer or not self.reader:
            return
        result = None
        try:
            # handshake
            self.send_handshake()
            data = await self.reader.readexactly(68)
            if not self.check_handshake(data):
                logging.error('Handshake response error: %s', data)
                return

            # ext handshake
            self.send_ext_handshake()
            pieces_list = None
            pieces_count = 0
            metadata_size = 0
            while True:
                data = await self.read_message()
                if data.read(1) != BT_MSG_ID:
                    return
                if data.read(1) == EXT_HANDSHAKE_ID:
                    if pieces_list is not None:
                        logging.error("Metadata pended before handshake!")
                        return
                    packet = data.read()
                    meta_dict = bencoder.bdecode(packet)
                    logging.debug('meta_dict: %s', dict(meta_dict))
                    ut_metadata, metadata_size = meta_dict[b'm'][b'ut_metadata'], meta_dict.get(b'metadata_size', 0)
                    if not metadata_size:
                        logging.error('Empty metadata size')
                        return
                    pieces_count = int(math.ceil(metadata_size / BLOCK_SIZE))
                    pieces_list = [b''] * pieces_count
                    for piece_index in range(pieces_count):
                        self.request_metadata(ut_metadata, piece_index)
                    continue
                else:
                    if pieces_list is None:
                        logging.error("metadata_size not defined")
                        return
                    # piece是个控制块，根据控制块下载数据
                    packet = data.read()
                    piece_dict, index = bencoder.decode_dict(packet, 0)
                    if piece_dict[b'msg_type'] != 1:
                        continue
                    piece_index = piece_dict[b'piece']
                    piece_data = packet[len(packet) - index:]
                    if not self.check_piece(piece_data, piece_index, pieces_count, metadata_size):
                        return
                    pieces_list[piece_index] = piece_data
                    if self.pieces_done(pieces_list):
                        break
                    continue
            pieces = b''.join(pieces_list)
            sha1_encoder = hashlib.sha1()
            sha1_encoder.update(pieces)
            if self.infohash != sha1_encoder.digest():
                logging.error("Infohash %s not equal to info sha1 %s", self.infohash.hex(), sha1_encoder.hexdigest())
                return
            result = bencoder.bdecode(pieces)
            del pieces
            gc.collect()
            logging.debug("Fetched metadata")
            return result
        except Exception as e:
            logging.exception(e)
        finally:
            self.close()  # 确保关闭socket

    def close(self):
        self.writer.close()


class Crawler(Maga):
    async def handle_get_peers(self, infohash, addr):
        logging.debug(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # await self.redis.lpush("infohash", infohash)
        logging.debug(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")
        fetcher = MetaFetcher(bytes.fromhex(infohash), peer_addr)
        await fetcher.init()
        task = asyncio.ensure_future(fetcher.get_metadata())
        task.add_done_callback(self.handle_metadata)
        await fetcher.get_metadata()

    def handle_metadata(self, future):
        data = future.result


if __name__ == '__main__':
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except:
        pass
    crawler = Crawler()
    # Set port to 0 will use a random available port
    crawler.run(port=0)
