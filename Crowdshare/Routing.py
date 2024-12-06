#!/usr/bin/env python3
# from p2play.Node import Node
# from p2play.Bucket import Bucket
# from p2play.ClosestNodesTraverser import ClosestNodesTraverser

from Crowdshare.Node import Node
from Crowdshare.Bucket import Bucket
from Crowdshare.ClosestNodesTraverser import ClosestNodesTraverser

from random import randint
import heapq
import asyncio
import logging

logger = logging.getLogger(__name__)

class RoutingTable:
    def __init__(self, id: int , k: int, protocol):
        self.protocol  = protocol
        self.node_id   = id
        self.k         = k
        self.k_buckets = [Bucket((0, 2**160), self.k)]
    
    @property
    def refresh_list(self) -> list:
        '''
        Returns a list of ID values that need to be refreshed
        '''
        for bucket in self.k_buckets:
            if bucket.needs_refresh():
                yield randint(*bucket.range)
                
    def greet(self, node_to_greet: Node) -> None:
        '''
        Try to add node to routing table

        Sec 2.5 of paper
        '''
        if self.new_node(node_to_greet):
            logger.debug("In greet, greeting new node with id: %s", node_to_greet.id)
            self.add_node(node_to_greet)
        # TODO: Send the new node the key-value pairs it should be storing


    def new_node(self, node: Node) -> bool:
        '''
        Checks if the node is new to the routing table.
        Params: Node
        Returns: bool
        '''
        bucket = self.get_bucket(node.id)
        if node.id in bucket:
            return False
        return True
    
    def add_node(self, node: Node) -> None:
        '''
        Adds a node to the routing table.
        Params: Node
        Returns: None
        '''
        index = self.get_bucket_index(node.id)
        bucket = self.k_buckets[index]

        # If bucket is not full then simply add the node and return
        if bucket.add_node(node):
            return
        
        # If the bucket is full and if the buckets range includes the node's id, then split the bucket
        # Sec 4.2
        if bucket.in_range(node.id) or bucket.depth % 5:
            logger.debug(f'Splitting bucket {index}')
            self.split_bucket(index, node)
            self.add_node(node)
        else:
            task = self._ping_LRU(bucket, node)
            asyncio.ensure_future(task)
    
    def remove_node(self, node: Node) -> None:
        '''
        Removes a node from the routing table
        '''
        bucket = self.get_bucket(node.id)
        bucket.remove_node(node)
    
    def split_bucket(self, bucket_index: int, node: Node) -> None:
        '''
        Split the bucket at the given index.
        Params: bucket_index (int), node (Node)
        Returns: None
        '''
        left, right = self.k_buckets[bucket_index].split()
        self.k_buckets[bucket_index] = left
        self.k_buckets.insert(bucket_index + 1, right)
    
    async def _ping_LRU(self, bucket: Bucket, node_to_add: Node):
        '''
        Pings the least recently used node in the bucket. If the node is unresponsive then remove it from the bucket and add the new node.
        Params: Bucket, Node
        Returns: None
        '''
        # If the node is unresponsive then remove it from the bucket
        result = await self.protocol.ping(bucket.oldest)
        if not result[1]:
            logger.info(f'Node {bucket.oldest.id} is unresponsive. Removing it from the bucket.')
            logger.info(f'Adding {node_to_add.id} to the bucket.')
            self.add_node(node_to_add)

    def find_kclosest(self, target_id: int, limit: int = 20, exclude: Node =None) -> list[Node]:
        '''
        Returns the k closest nodes to the target_id.
        Params: target_id - int, limit - int
        Returns: list of Node objects
        '''
        target       = Node(_id=target_id)
        bucket_index = self.get_bucket_index(target.id)
        closest_list = []

        logger.debug("In find_kclosest with target: %s, limit: %s, and exclude: %s", target, limit, exclude)

        for node in ClosestNodesTraverser(self.k_buckets, bucket_index):
            if exclude and (node.id == exclude.id):
                logger.debug("Excluding node %s", node)
                continue
            if node.id == target.id:
                continue
            distance = target.distance(node)
            heapq.heappush(closest_list, (distance, node.id, node.ip, node.port))
            if len(closest_list) == limit:
                break 
        
        return [Node(*args) for _, *args in heapq.nsmallest(limit, closest_list)]
  
    
    def not_in_bucket(self, bucket, node):
        for n in bucket:
            if n.id == node.id:
                return False
        return True

    def get_bucket_index(self, node_id: int):
        '''
        Get the index of the bucket that the node belongs to.
        '''
        for index, bucket in enumerate(self.k_buckets):
            if node_id < bucket.range[1]:
                return index
        return None
    
    def get_bucket(self, node_id: int) -> Bucket:
        '''
        Get the bucket that the node belongs to.
        '''
        return self.k_buckets[self.get_bucket_index(node_id)]
    
    def __repr__(self):
        result = []
        for i, buck in enumerate(self.k_buckets):
            if len(buck) == 0:
                continue
            result.append(f'Bucket {i}: {list(buck.nodes.keys())}')
        return '\n'.join(result)

    def __len__(self):
        return sum([len(bucket) for bucket in self.k_buckets])
    
    def __contains__(self, node_id: int):
        bucket_index = self.get_bucket_index(node_id)
        bucket = self.k_buckets[bucket_index]
        for node in bucket:
            if node.id == node_id:
                return True
        return False
        