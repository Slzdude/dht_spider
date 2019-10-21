import logging

import aioredis

from mega import Maga

logging.basicConfig(level=logging.INFO)


# Or, if you want to have more control

class Crawler(Maga):
    async def init(self):
        self.redis = await aioredis.create_redis_pool('redis://localhost')

    async def close(self):
        self.redis.close()

    async def handle_get_peers(self, infohash, addr):
        await self.redis.lpush(infohash)
        logging.info(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        await self.redis.lpush(infohash)
        logging.info(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")


crawler = Crawler()
# Set port to 0 will use a random available port
crawler.run(port=0)
