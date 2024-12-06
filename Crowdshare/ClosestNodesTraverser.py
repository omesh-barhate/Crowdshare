from __future__ import annotations
from Crowdshare.Node import Node
import logging

logger = logging.getLogger(__name__)

class ClosestNodesTraverser:
    '''
    This class is used to traverse the routing table from a given bucket for the closest nodes.
    '''
    def __init__(self, buckets, index) -> None:
        self.curr_nodes    = buckets[index].curr_nodes
        self.left_buckets  = buckets[:(index)]
        self.right_buckets = buckets[(index + 1):]
        self.dir           = 1

    def __iter__(self) -> ClosestNodesTraverser:
        return self
    
    def __next__(self) -> Node:
        '''
        Try to get a node from the left bucket then the right bucket. If no nodes, then keep on repeating this process until all buckets are exhausted.
         '''
        if self.curr_nodes:
            return self.curr_nodes.pop()

        if not self.right_buckets and not self.left_buckets:
            raise StopIteration
    
        # If the direction is left and there are still left buckets, then set the current nodes to the left bucket's nodes. Or if there are no right buckets, then do the same.
        if self.dir and self.left_buckets or not self.right_buckets:
            self.curr_nodes = self.left_buckets.pop().curr_nodes
            self.dir           = 0
        # If the direction is right and there are still right buckets, then set the current nodes to the right bucket's nodes. Or if there are no left buckets, then do the same.
        else:
            self.curr_nodes = self.right_buckets.pop(0).curr_nodes
            self.dir           = 1
        return next(self)