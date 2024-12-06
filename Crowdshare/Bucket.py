import time
from Crowdshare.Node import Node
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

class Bucket:
    def __init__(self, _range: tuple[int, int], k: int) -> None:
        self.range             = _range
        self.k                 = k
        self.nodes             = OrderedDict()
        self.replacement_cache = OrderedDict()
        self.last_touched      = time.monotonic_ns()
    
    def touch(self) -> None:
        '''
        Updates the last_touched attribute to the current time.
        Params: None
        Returns: None
        '''
        self.last_touched = time.monotonic_ns()
    
    def add_node(self, node: Node) -> bool:
        '''
        Adds a node to the bucket. If the bucket is full, the node is added to the replacement cache.
        Params: node - Node object
        Returns: bool
        '''
        if node.id in self.nodes:
            del self.nodes[node.id]
            self.nodes[node.id] = node
        elif len(self.nodes) < self.k:
            self.nodes[node.id] = node
        else:
            logger.debug(f'Bucket full, adding node %s to replacement cache', node.id)
            self._add_replacement(node)
            return False
        return True

    def remove_node(self, node: Node) -> bool:
        '''
        Removes a node from the bucket.
        Params: node - Node object
        Returns: bool
        '''
        # Remove the node from the replacement cache if it exists
        if node.id in self.replacement_cache:
            del self.replacement_cache[node.id]
            
        if node.id in self.nodes:
            del self.nodes[node.id]

            # If there are nodes in the replacement then add the first one to the bucket
            if self.replacement_cache:
                _id, node = self.replacement_cache.popitem()
                self.nodes[_id] = node
    
    def _add_replacement(self, node: Node) -> bool:
        '''
        Adds a node to the replacement cache.
        Params: node - Node object
        Returns: bool

        # Sec 4.1
        '''
        if node.id in self.replacement_cache:
            del self.replacement_cache[node.id]
        self.replacement_cache[node.id] = node
        # If the replacement cache is full, remove the oldest node	
        while len(self.replacement_cache) > self.k * 3:
            self.replacement_cache.popitem(last=False)

    def split(self) -> tuple:
        '''
        Splits the bucket into two buckets based on the midpoint of the range.
        Params: None
        Returns: tuple

        # Sec 2.4
        If bucket is full and if the buckets range includes the node's id, then split the bucket
        where the contents of the bucket are split between the two new buckets.
        
        '''
        midpoint = (self.range[0] + self.range[1]) // 2
        left     = Bucket((self.range[0], midpoint), self.k)
        right    = Bucket((midpoint + 1, self.range[1]), self.k)

        for node in self.all_nodes:
            bucket = left if node.id <= midpoint else right
            bucket.add_node(node)

        return left, right
    
    def in_range(self, _id: int) -> bool:
        '''
        Checks if the given id is in the range of the bucket.
        Params: _id - int
        Returns: bool
        '''
        return _id in range(self.range[0], self.range[1])

    @property
    def all_nodes(self) -> list[Node]:
        return list(self.nodes.values()) + list(self.replacement_cache.values())
    
    @property
    def curr_nodes(self) -> list[Node]:
        return list(self.nodes.values())
    
    @property
    def oldest(self) -> Node:
        return list(self.nodes.values())[0]
    
    @property
    def depth(self) -> int:
        '''
        Per sec 4.2
        Depth is the length of the prefix shared by all nodes in the buckets range.
        '''
        # Get all node ids
        node_ids = [node.id for node in self.all_nodes]
        # Turn ids into binary strings
        bin_strings = [bin(x)[2:].zfill(160) for x in node_ids]
        
        shortest = min(bin_strings, key=len)
        # Use zip to compare each bit of each id
        for i, bits in enumerate(zip(*bin_strings)):
            if len(set(bits)) > 1:
                return i

        # If all ids are same to the given length then return the length
        return len(shortest)
    
    def needs_refresh(self) -> bool:
        '''
        Returns whether there have been any lookups in this bucket within the last hour.
        '''
        hour = 3600
        return time.monotonic_ns() - self.last_touched > hour * 1e9

    def __contains__(self, node_id: int) -> bool:
        return node_id in self.nodes
    
    def __len__(self) -> int:
        return len(self.nodes)
    
    def __repr__(self):
        print(f'Bucket: {self.range}')

            
        
    