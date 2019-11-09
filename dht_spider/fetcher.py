import asyncio
import gc
import hashlib
import math
import struct
from io import BytesIO

import bencoder
import six

from dht_spider.logger import setup_logger
from dht_spider.utils import random_node_id

BLOCK_SIZE = 16.0 * 1024
bt_header = b'\x13BitTorrent protocol'
ext_bytes = b'\x00\x00\x00\x00\x00\x10\x00\x01'
HANDSHAKE_PREFIX = bt_header + ext_bytes

BT_MSG_ID = b'\x14'
EXT_HANDSHAKE_ID = b'\x00'
logger = setup_logger("Mega Spider")


class BaseMetaFetcher:
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
            logger.error('Packet format error')
            return False
        if packet[28:48] != self.infohash:
            logger.error('Infohash not match')
            return False
        return True

    def send_handshake(self):
        logger.debug("send handshake")
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
        logger.debug("send ext handshake")
        # msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencoder.bencode({'m': {'ut_metadata': 1}})
        msg = b'\x14\x00d1:md11:ut_metadatai1eee'
        self.send_message(msg)

    def request_metadata(self, ut_metadata, piece):
        msg = BT_MSG_ID + six.int2byte(ut_metadata) + bencoder.bencode({'msg_type': 0, 'piece': piece})
        self.send_message(msg)

    @staticmethod
    def pieces_done(pieces):
        for piece in pieces:
            if len(piece) == 0:
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
        try:
            # handshake
            self.send_handshake()
            try:
                data = await self.reader.readexactly(68)
            except asyncio.IncompleteReadError:
                logger.debug('Handshake failed')
                return
            if not self.check_handshake(data):
                logger.error('Handshake response error: %s', data)
                return

            # ext handshake
            self.send_ext_handshake()
            pieces_list = None
            pieces_count = 0
            metadata_size = 0
            while True:
                data = await self.read_message()
                if data.read(1) != BT_MSG_ID:
                    continue
                if data.read(1) == EXT_HANDSHAKE_ID:
                    if pieces_list is not None:
                        logger.error("Metadata pended before handshake!")
                        return
                    packet = data.read()
                    meta_dict = bencoder.bdecode(packet)
                    ut_metadata, metadata_size = meta_dict[b'm'][b'ut_metadata'], meta_dict.get(b'metadata_size', 0)
                    if not metadata_size:
                        logger.error('Empty metadata size: %s', dict(meta_dict))
                        return
                    pieces_count = int(math.ceil(metadata_size / BLOCK_SIZE))
                    pieces_list = [b''] * pieces_count
                    logger.debug("request metadata pieces count [%d]", pieces_count)
                    for piece_index in range(pieces_count):
                        self.request_metadata(ut_metadata, piece_index)
                else:
                    if pieces_list is None:
                        logger.error("metadata_size not defined")
                        return
                    # piece是个控制块，根据控制块下载数据
                    packet = data.read()
                    piece_dict, index = bencoder.decode_dict(packet, 0)
                    if piece_dict[b'msg_type'] != 1:
                        logger.debug("Message type [%d]", piece_dict[b'msg_type'])
                        continue
                    piece_index = piece_dict[b'piece']
                    piece_data = packet[index:]
                    if not self.check_piece(piece_data, piece_index, pieces_count, metadata_size):
                        logger.error("Piece check failed [%s]", [piece_data, piece_index, pieces_count, metadata_size])
                        return
                    pieces_list[piece_index] = piece_data
                    if self.pieces_done(pieces_list):
                        break
                continue
            logger.debug("Merge pieces together")
            pieces = b''.join(pieces_list)
            sha1_encoder = hashlib.sha1()
            sha1_encoder.update(pieces)
            if self.infohash != sha1_encoder.digest():
                logger.error("Infohash %s not equal to info sha1 %s", self.infohash.hex(), sha1_encoder.hexdigest())
                return
            result = bencoder.bdecode(pieces)
            del pieces
            gc.collect()
            logger.debug("Fetched metadata")
            return result
        except Exception as e:
            logger.exception(e)
        finally:
            self.close()

    def close(self):
        self.writer.close()
