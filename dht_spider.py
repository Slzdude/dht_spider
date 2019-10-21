from gevent import monkey

monkey.patch_all()

from gevent.server import DatagramServer
import gevent
import socket
from hashlib import sha1
from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Timer, Thread
from gevent import sleep
from collections import deque
import random

import sys

sys.path.insert(0, '.')

from bencode import bencode, bdecode
import random

BOOTSTRAP_NODES = [
    ('59.136.201.207', 16849),
    ('37.214.43.181', 13227),
    ('197.101.100.95', 52690),
    ('59.136.201.207', 16849),
    ('197.101.100.95', 52690),
    ('123.248.31.6', 53152),
    ('76.110.37.254', 6881),
    ('87.98.162.88', 6881),
    ("router.bittorrent.com", 6881),
    ("router.bittorrent.com", 8991),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("67.215.246.10", 6881),
    ("82.221.103.244", 6881),
    ("23.21.224.150", 6881)
]
with open("nodes.txt") as f:
    nodes = f.readlines()
    for n in nodes:
        n = n.strip()
        ip, port = n[1:-1].split(',')
        port = int(port)
        BOOTSTRAP_NODES.append((ip, port))
print(BOOTSTRAP_NODES[-3:])

TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
MONITOR_INTERVAL = 60
TOKEN_LENGTH = 2


def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in range(length))


def random_id():
    h = sha1()
    e = entropy(20).encode()
    h.update(e)
    return bytes(h.digest())


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n

    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack("!H", nodes[i + 24:i + 26])[0]
        n.append((nid, ip, port))

    return n


def get_neighbor(target, nid, end=10):
    return target[:end] + nid[end:]


class KNode(object):

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTServer(DatagramServer):
    def __init__(self, max_node_qsize, bind_ip):
        s = ':' + str(bind_ip)
        self.bind_ip = bind_ip
        print(s)
        DatagramServer.__init__(self, s)

        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
            "ping": self.on_ping,
        }
        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = deque(maxlen=max_node_qsize)
        self.peers = []
        self.message_num = {"ping": 0, "find_node": 0, "get_peers": 0, "announce_peer": 0, "others": 0}
        # self.fnd = open("nodes.txt","w+")
        self.peer_file = open("peers.txt", "w+")

    def handle(self, data, address):  #
        #        print(data, address)
        try:
            #            print(data.decode())
            msg = bdecode(data)
            #            print(msg, address)
            #            self.fnd.write('('+address[0]+','+str(address[1])+')\n')
            #            self.fnd.flush()
            # print("saved")
            self.on_message(msg, address)
        except Exception:
            pass

    def monitor(self):
        while True:
            print('nodes={}, peers={}'.format(len(self.nodes), len(self.peers)))
            print(self.message_num)
            sleep(MONITOR_INTERVAL)

    def send_krpc(self, msg, address):
        try:
            #            print(bencode(msg), address)
            self.socket.sendto(bencode(msg), address)
        except Exception:
            pass

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid, self.nid) if nid else self.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": 'aa',  # tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id()
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        # print("join_DHT")
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def re_join_DHT(self):
        while True:
            if len(self.nodes) == 0:
                self.join_DHT()
            sleep(RE_JOIN_DHT_INTERVAL)

    def auto_send_find_node(self):

        wait = max(1.0 / self.max_node_qsize / 5.0, 0.01)
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                pass
            sleep(wait)

    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg["r"]["nodes"])
        # print('find node' + len(nodes))
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            # if ip == self.bind_ip: continue
            if port < 1 or port > 65535: continue
            n = KNode(nid, ip, port)
            self.nodes.append(n)

    def on_message(self, msg, address):
        if 'q' in msg:
            if msg['q'] in self.message_num:
                self.message_num[msg['q']] += 1
            else:
                self.message_num['others'] += 1
        try:
            if msg["y"] == "r":
                if 'nodes' in msg["r"]:
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

    def on_ping(self, msg, address):
        try:
            msg = {
                "t": msg['t'],
                "y": 'r',
                "r": {
                    "id": self.nid
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_get_peers_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = infohash[:TOKEN_LENGTH]
            info = infohash.hex().upper() + '|' + address[0]
            # if random.random() > 0.95:
            #    print(info)
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(infohash, self.nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address):
        try:
            # print('announce peer')
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]

            if infohash[:TOKEN_LENGTH] == token:
                if "implied_port" in msg["a"] and msg["a"]["implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                    if port < 1 or port > 65535: return
                info = infohash.hex().upper()
                # print(info)
                # self.peers.append(info)
                self.peer_file.write(info + '\n')
                self.peer_file.flush()
        except Exception as e:
            print(e)
            pass
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            tid = msg["t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            print('error')
            pass

    def ok(self, msg, address):
        try:
            tid = msg["t"]
            nid = msg["a"]["id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(nid, self.nid)
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass


if __name__ == '__main__':
    sniffer = DHTServer(5000, 6882)
    gevent.spawn(sniffer.auto_send_find_node)
    gevent.spawn(sniffer.re_join_DHT)
    gevent.spawn(sniffer.monitor)
    # sniffer.join_DHT()
    print('Receiving datagrams on :6882')
    sniffer.serve_forever()
