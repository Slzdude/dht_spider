import asyncio

from dht_spider.fetcher import BaseMetaFetcher
from dht_spider.logger import setup_logger
from dht_spider.mega import Maga

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass

logger = setup_logger("Mega Spider")

queue = asyncio.Queue()


class Fetcher(BaseMetaFetcher):
    async def get_metadata(self):
        result = await super(Fetcher, self).get_metadata()
        await queue.put((self.infohash.hex(), result))


async def handle_metadata():
    while True:
        try:
            infohash, metadata = await queue.get()
            if not metadata.get(b'name'):
                continue
            data = {
                "infohash": infohash,
                "name": str(metadata[b'name'], "utf8"),
                "length": 0
            }
            if metadata.get(b'files'):
                data['files'] = []
                for file in metadata[b'files']:
                    data['files'].append({
                        "path": str(b'/'.join(file[b'path']), "utf8"),
                        "length": file[b'length']
                    })
                    data['length'] += file[b'length']
            elif metadata.get(b'length'):
                data["length"] = metadata[b'length']
            logger.info(data['name'])
        except Exception as e:
            logger.exception("Error when parse metadata", e)


class Crawler(Maga):
    async def handle_get_peers(self, infohash, addr):
        logger.debug(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        logger.debug(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")
        fetcher = Fetcher(infohash, peer_addr)
        await fetcher.init()
        asyncio.ensure_future(fetcher.get_metadata())


if __name__ == '__main__':
    crawler = Crawler()
    asyncio.ensure_future(handle_metadata())
    crawler.run(port=0)
