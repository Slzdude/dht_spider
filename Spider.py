import hashlib
import os.path
import random
import socket
from queue import Queue, Empty
from struct import unpack, pack
from threading import Thread
from time import sleep

import MetadataInquirer
from libs import pymmh3, decodeh
from libs.SQLiteUtil import SQLiteUtil
from libs.bencode import bencode, bdecode
from libs.logger import setup_logger

logger = setup_logger("DHTSpider")


def random_id():
    return hashlib.sha1(b''.join(bytes(random.randint(0, 255)) for _ in range(20))).digest()


def get_neighbor_id(target, end=10):
    return target[:end] + random_id()[end:]


# node节点结构
class KNode(object):
    def __init__(self, nid, ip=None, port=None):
        self.nid = nid
        self.ip = ip
        self.port = port

    def __eq__(self, other):
        return other.nid == self.nid

    def __hash__(self):
        return hash(self.nid)

    @staticmethod
    def decode_nodes(nodes):
        """
        解析node串，每个node长度为26，其中20位为nid，4位为ip，2位为port
        数据格式: [ (node ID, ip, port),(node ID, ip, port),(node ID, ip, port).... ]
        """
        n = []
        length = len(nodes)
        if (length % 26) != 0:
            return n

        for i in range(0, length, 26):
            nid = nodes[i:i + 20]
            ip = socket.inet_ntoa(nodes[i + 20:i + 24])
            port = unpack('!H', nodes[i + 24:i + 26])[0]
            n.append((nid, ip, port))

        return n

    @staticmethod
    def encode_nodes(nodes):
        """ Encode a list of (id, connect_info) pairs into a node_info """
        n = []
        for node in nodes:
            n.extend([node.nid, int(''.join(['%02X' % int(i) for i in node.ip.split('.')]), 16), node.port])
        return pack('!' + '20sIH' * len(nodes), *n)


class Spider(Thread):
    def __init__(self, bind_ip, bind_port, max_node_size):
        Thread.__init__(self)
        self.setDaemon(True)

        self.isSpiderWorking = True

        self.nid = random_id()
        self.max_node_size = max_node_size
        self.node_list = []
        self.inquiry_info_queue = Queue()
        self.metadata_queue = Queue()

        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))

    def start(self):

        Thread(target=self.join_dht).start()
        Thread(target=self.receiver).start()
        Thread(target=self.sniffer).start()
        for _ in range(100):  # 防止inquiry_info_queue消费过慢
            Thread(target=self.inquirer).start()
        Thread(target=self.recorder).start()
        Thread.start(self)

    def stop(self):
        self.isSpiderWorking = False

    # 加入DHT网络
    def join_dht(self):
        # 起始node
        BOOTSTRAP_NODES = [
            ('router.utorrent.com', 6881),
            ('router.bittorrent.com', 6881),
            ('dht.transmissionbt.com', 6881)
        ]
        for _ in range(20):
            if len(self.node_list) == 0:
                self.send_find_node(random.choice(BOOTSTRAP_NODES), self.nid)
            if self.isSpiderWorking:
                sleep(10)

    # 获取Node信息
    def sniffer(self):
        while self.isSpiderWorking:
            for _ in range(200):
                if not self.isSpiderWorking:
                    break
                if len(self.node_list) == 0:
                    sleep(1)
                    continue
                try:
                    # 伪装成目标相邻点在查找
                    node = self.node_list.pop(0)  # 线程安全 global interpreter lock
                    self.send_find_node((node.ip, node.port), get_neighbor_id(node.nid))
                except:
                    pass
            if self.isSpiderWorking:
                sleep(10)

    # 接收ping, find_node, get_peers, announce_peer请求和find_node回复
    def receiver(self):
        while self.isSpiderWorking:
            (data, address) = self.ufd.recvfrom(65536)
            logger.debug(f'receive udp packet length: {len(data)}')
            msg = bdecode(data)
            if msg[b'y'] == b'r':
                if b'nodes' in msg[b'r']:
                    logger.debug("Process Find Node")
                    self.process_find_node_response(msg)
            elif msg[b'y'] == b'q':
                if msg[b'q'] == b'ping':
                    logger.debug("Process Ping")
                    self.send_pong(msg, address)
                elif msg[b'q'] == b'find_node':
                    logger.info("Process Find Node")
                    self.process_find_node_request(msg, address)
                elif msg[b'q'] == b'get_peers':
                    logger.info("Process Get Peers")
                    self.process_get_peers_request(msg, address)
                elif msg[b'q'] == b'announce_peer':
                    logger.info("Process Announce Peer")
                    self.process_announce_peer_request(msg, address)
                else:
                    logger.info("Message" + msg[b'q'].decode())

    # 发送本节点状态正常信息
    def send_pong(self, msg, address):
        msg = {
            't': msg[b't'],
            'y': 'r',
            'r': {'id': self.nid}
        }
        self.ufd.sendto(bencode(msg), address)

    # 发送查询节点请求信息
    def send_find_node(self, address, nid, target_id=random_id()):
        msg = {
            't': ''.join(chr(random.randint(0, 255)) for _ in range(2)),
            'y': 'q',
            'q': 'find_node',
            'a': {'id': nid, 'target': target_id}
        }
        self.ufd.sendto(bencode(msg), address)

    # 处理查询节点请求的回复信息，用于获取新的有效节点
    def process_find_node_response(self, res):
        if len(self.node_list) > self.max_node_size:  # 限定队列大小
            return
        nodes = KNode.decode_nodes(res[b'r'][b'nodes'])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20:
                continue
            if nid == self.nid:
                continue  # 排除自己
            if ip == self.bind_ip:
                continue
            if port < 1 or port > 65535:
                continue
            self.node_list.append(KNode(nid, ip, port))

    # 回应find_node请求信息
    def process_find_node_request(self, req, address):
        msg = {
            't': req[b't'],
            'y': 'r',
            'r': {'id': get_neighbor_id(self.nid), 'nodes': KNode.encode_nodes(self.node_list[:8])}
        }
        self.ufd.sendto(bencode(msg), address)

    # 回应get_peer请求信息
    def process_get_peers_request(self, req, address):
        info_hash = req[b'a'][b'info_hash']
        msg = {
            't': req[b't'],
            'y': 'r',
            'r': {
                'id': get_neighbor_id(info_hash, 3),
                'nodes': KNode.encode_nodes(self.node_list[:8]),
                'token': info_hash[:4]  # 自定义token，例如取info_hash最后四位
            }
        }
        self.ufd.sendto(bencode(msg), address)

    # 处理声明下载peer请求信息，用于获取有效的种子信息
    def process_announce_peer_request(self, req, address):
        info_hash = req[b'a'][b'info_hash']
        token = req[b'a'][b'token']
        if info_hash[:4] == token:  # 自定义的token规则校验
            if b'implied_port' in req[b'a'] and req[b'a'][b'implied_port'] != 0:
                port = address[1]
            else:
                port = req[b'a'][b'port']
            if port < 1 or port > 65535:
                return

        logger.info('announce_peer:' + info_hash.encode('hex') + ' ip:' + address[0])
        self.inquiry_info_queue.put((info_hash, (address[0], port)))  # 加入元数据获取信息队列

        self.send_pong(req, address)

    # 查询种子信息
    def inquirer(self):
        while self.isSpiderWorking:
            # 只用于保证局部无重复，实际数据唯一性通过数据库唯一键保证
            filter = BloomFilter(5000, 5)
            for _ in range(1000):
                if self.isSpiderWorking:
                    try:
                        announce = self.inquiry_info_queue.get(timeout=0.3)
                        if filter.add(announce[0] + announce[1][0]):
                            # threads for download metadata
                            t = Thread(target=MetadataInquirer.inquire,
                                       args=(announce[0], announce[1], self.metadata_queue, 7))  # 超时时间不要太长防止短时间内线程过多
                            t.start()
                    except:
                        pass

    # 记录种子信息
    def recorder(self):
        db_name = 'metadata.db'
        need_create_table = False
        if not os.path.exists(db_name):
            need_create_table = True
        sqlite_util = SQLiteUtil(db_name)

        if need_create_table:
            sqlite_util.executescript(
                'create table "matadata" ("hash" text primary key not null,"name"  text,"size"  text);')
        while self.isSpiderWorking:
            try:
                metadata = self.metadata_queue.get(timeout=0.5)
                name = metadata[b'name']
                name = decodeh.decode(name)
                sqlite_util.execute('insert into matadata (hash,name,size)values (?,?,?);',
                                    (metadata[b'hash'], name, metadata[b'size']))
            except Empty:
                sleep(0.5)
            except Exception as e:
                logger.exception(e)


# 简化版布隆过滤器
class BloomFilter(object):
    def __init__(self, size, hash_count):
        self.bit_number = 0  # 初始化
        self.size = size
        self.hash_count = hash_count

    def add(self, item):
        for i in range(self.hash_count):
            index = pymmh3.hash(item, i) % self.size
            if not (self.bit_number >> index) & 1:  # 如果是0则是新的，返回True
                for j in range(self.hash_count):
                    index1 = pymmh3.hash(item, j) % self.size
                    self.bit_number |= 1 << index1
                return True
        return False


if __name__ == '__main__':
    spiderList = []
    for i in range(3):
        spider = Spider('0.0.0.0', 8087 + i, max_node_size=1500)  # 需保证有公网ip且相应端口入方向通畅
        spider.start()
        spiderList.append(spider)
        sleep(1)

    sleep(60 * 60 * 8)  # 持续运行一段时间

    for spider in spiderList:
        spider.stop()
        spider.join()
