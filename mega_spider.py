import logging

from mega import Maga

logging.basicConfig(level=logging.INFO)


# Or, if you want to have more control

class Crawler(Maga):
    async def handle_get_peers(self, infohash, addr):
        logging.info(f"Receive get peers message from DHT {addr}. Infohash: {infohash}.")

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        logging.info(f"Receive announce peer message from DHT {addr}. Infohash: {infohash}. Peer address:{peer_addr}")


crawler = Crawler()
# Set port to 0 will use a random available port
crawler.run(port=8087)