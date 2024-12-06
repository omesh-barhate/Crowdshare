from __future__ import annotations
from random     import getrandbits
from hashlib    import sha1
import logging

logger = logging.getLogger(__name__)

class Node:
    def __init__(self, _id: int = None, ip: str = None, port: int = None) -> None:
        self.id      = _id if _id != None else self.generate_id()
        self.ip      = ip
        self.port    = port

    def generate_id(self) -> int:
        rand_num = getrandbits(160)
        return int(sha1(rand_num.to_bytes(160, byteorder="big")).hexdigest(), 16)
    
    def distance(self, other: Node) -> int:
        return self.id ^ other.id
    
    def same_addr(self, node: Node) -> bool:
        logger.debug("Comparing %s to %s with self: (%s:%s) and node: (%s:%s)", self, node, self.ip, self.port, node.ip, node.port)
        return self.ip == node.ip and self.port == node.port

    def __repr__(self):
        return f'Node({self.id})'