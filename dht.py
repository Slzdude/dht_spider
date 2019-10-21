import gc
import math
import os
import signal
import socket
import sys
import threading
from collections import deque
from hashlib import sha1
from queue import Queue
from random import randint
from socket import inet_ntoa
from struct import pack, unpack
from threading import Timer, Thread
from time import sleep, time

from elasticsearch import Elasticsearch

from libs.bencode import bencode, bdecode

es = Elasticsearch()

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)
TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2

BT_PROTOCOL = "BitTorrent protocol"
BT_MSG_ID = 20
EXT_HANDSHAKE_ID = 0
thread_num = 10000


# class watch start
class Watcher:
    def __init__(self):
        self.child = os.fork()
        if self.child == 0:
            return
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            print('[EXIT] Control-C')
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError:
            pass


# class watch end

def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in xrange(length))


def random_id():
    h = sha1()
    h.update(entropy(20))
    return h.digest()


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


def timer(t, f):
    Timer(t, f).start()


def get_neighbor(target, nid, end=10):
    return target[:end] + nid[end:]


def send_packet(the_socket, msg):
    the_socket.send(msg)


def send_message(the_socket, msg):
    msg_len = pack(">I", len(msg))
    send_packet(the_socket, msg_len + msg)


def send_handshake(the_socket, infohash):
    bt_header = chr(len(BT_PROTOCOL)) + BT_PROTOCOL
    ext_bytes = "\x00\x00\x00\x00\x00\x10\x00\x00"
    peer_id = random_id()
    packet = bt_header + ext_bytes + infohash + peer_id

    send_packet(the_socket, packet)


def check_handshake(packet, self_infohash):
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
    if infohash != self_infohash:
        return False

    return True


def send_ext_handshake(the_socket):
    msg = chr(BT_MSG_ID) + chr(EXT_HANDSHAKE_ID) + bencode({"m": {"ut_metadata": 1}})
    send_message(the_socket, msg)


def request_metadata(the_socket, ut_metadata, piece):
    """bep_0009"""
    msg = chr(BT_MSG_ID) + chr(ut_metadata) + bencode({"msg_type": 0, "piece": piece})
    send_message(the_socket, msg)


def get_ut_metadata(data):
    try:
        ut_metadata = "_metadata"
        index = data.index(ut_metadata) + len(ut_metadata) + 1
        return int(data[index])
    except Exception as e:
        pass


def get_metadata_size(data):
    metadata_size = "metadata_size"
    start = data.index(metadata_size) + len(metadata_size) + 1
    data = data[start:]
    return int(data[:data.index("e")])


def recvall(the_socket, timeout=15):
    the_socket.setblocking(0)
    total_data = []
    data = ""
    begin = time()

    while True:
        sleep(0.05)
        if total_data and time() - begin > timeout:
            break
        elif time() - begin > timeout * 2:
            break
        try:
            data = the_socket.recv(1024)
            if data:
                total_data.append(data)
                begin = time()
        except Exception:
            pass
    return "".join(total_data)


def ip_black_list(ipaddress):
    black_lists = ['45.32.5.150', '45.63.4.233']
    if ipaddress in black_lists:
        return True
    return False


def download_metadata(address, infohash, timeout=15):
    if ip_black_list(address[0]):
        return
    try:
        the_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        the_socket.settimeout(15)
        the_socket.connect(address)

        # handshake
        send_handshake(the_socket, infohash)
        packet = the_socket.recv(4096)

        # handshake error
        if not check_handshake(packet, infohash):
            try:
                the_socket.close()
            except:
                return
            return

        # ext handshake
        send_ext_handshake(the_socket)
        packet = the_socket.recv(4096)

        # get ut_metadata and metadata_size
        ut_metadata, metadata_size = get_ut_metadata(packet), get_metadata_size(packet)
        # print('ut_metadata_size: ', metadata_size)

        # request each piece of metadata
        metadata = []
        for piece in range(int(math.ceil(metadata_size / (16.0 * 1024)))):
            request_metadata(the_socket, ut_metadata, piece)
            packet = recvall(the_socket, timeout)  # the_socket.recv(1024*17) #
            metadata.append(packet[packet.index("ee") + 2:])

        metadata = "".join(metadata)

        # vierfy metadata
        chech_metadata = sha1(str(metadata)).hexdigest()
        if chech_metadata.upper() != infohash.encode("hex").upper():
            print("[check infohash] failed.Check_sum:" + chech_metadata.upper())
            try:
                print("\r\n\r\n")
                the_socket.close()
            except:
                print("\r\n\r\n")
                return
            return
        else:
            print("[check infohash] successful.Check_sum:" + chech_metadata.upper())

        info = {}
        meta_data = bdecode(metadata)
        del metadata

        if meta_data.has_key('name'):
            info["name"] = meta_data["name"].strip()
        else:
            info["name"] = '--'

        if meta_data.has_key('length'):
            info['length'] = meta_data['length']

        if meta_data.has_key('creation date'):
            info['create_time'] = meta_data['creation date']
        else:
            info['create_time'] = 0

        info['popularity'] = 1

        if meta_data.has_key('files'):
            info['files'] = meta_data['files']
            for item in info['files']:
                if item.has_key('length'):
                    info['length'] += item['length']
        else:
            info['files'] = [{
                "path": info["name"],
                "length": info['length']
            }]

        body = {
            'script': {
                'inline': 'ctx._source.popularity += 1',
                "lang": "painless"
            },
            'upsert': info
        }

        res = es.update(index='torrent_info', doc_type='common', id=infohash.encode("hex").lower(), body=body)
        print(str(res))
        del info
        gc.collect()

    except Exception as e:
        pass
    finally:
        the_socket.close()


class KNode(object):

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHT_Process(Thread):

    def __init__(self, master, ip, port, max_node_qsize):
        Thread.__init__(self)
        self.setDaemon(True)

        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = deque(maxlen=max_node_qsize)

        self.master = master
        self.ip = ip
        self.port = port

        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.ip, self.port))

        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)

    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except Exception:
            pass

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid, self.nid) if nid else self.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id()
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def re_join_DHT(self):
        if len(self.nodes) == 0:
            self.join_DHT()
        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)

    def run(self):
        self.re_join_DHT()
        while True:
            try:
                (data, address) = self.ufd.recvfrom(65536)
                msg = bdecode(data)
                self.on_message(msg, address)
            except Exception:
                pass

    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            if ip == self.ip: continue
            if port < 1 or port > 65535: continue
            n = KNode(nid, ip, port)
            self.nodes.append(n)

    def on_get_peers_request(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = infohash[:TOKEN_LENGTH]
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
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]

            if infohash[:TOKEN_LENGTH] == token:
                if msg["a"].has_key("implied_port") and msg["a"]["implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                    if port < 1 or port > 65535: return
                self.master.log(infohash, (address[0], port))
        except Exception:
            pass
        finally:
            self.ok(msg, address)

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

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if msg["r"].has_key("nodes"):
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q":
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

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
            pass

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                pass
            sleep(wait)


class Master(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.queue = Queue()

    def run(self):
        while True:
            self.downloadMetadata()

    def log(self, infohash, address=None):
        self.queue.put([address, infohash])

    def downloadMetadata(self):
        # 100 threads for download metadata
        for i in xrange(0, thread_num):
            # when queue is 0, wait for data to log
            if self.queue.qsize() == 0:
                sleep(1)
                continue
            announce = self.queue.get()
            t = threading.Thread(target=download_metadata, args=(announce[0], announce[1]))
            t.setDaemon(True)
            t.start()


# main
if __name__ == "__main__":
    # start watcher
    Watcher()
    # metadata process
    master = Master()
    master.start()
    # start DHT Network
    print('Starting Crawing on 6883')
    dht = DHT_Process(master, "0.0.0.0", 6883, max_node_qsize=40000)
    dht.start()
    thread.start_new_thread(dht.auto_send_find_node, ())
