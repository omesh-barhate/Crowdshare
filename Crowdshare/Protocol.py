import asyncio
import logging
import json
from   random import getrandbits
from Crowdshare.Node import Node
from Crowdshare.Kademila import KademilaFile

logger = logging.getLogger(__name__)

class CrowdshareProtocol(asyncio.DatagramProtocol):
    '''
    Protocol implementation for the P2Play protocol using asyncio to handle async IO.
    '''
    REQUEST_TIMEOUT = 2.0

    def __init__(self, client) -> None:
        '''
        Create a new P2PlayProtocol instance.
        '''
        self.client      = client
        self.transport   = None
        self.outstanding = {}

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data, addr) -> None:
        text = data.decode("utf-8").strip()

        try:
            response = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Received invalid JSON from %s: %s", addr, text)
            return

        match response["type"]:
            case "request":
                task = self.handle_request(response, addr)
                return asyncio.ensure_future(task)
            case "result":
                return self.handle_response(response, addr)
            case _:
                logger.warning("Received invalid message from %s: %s", addr, text)

    async def handle_request(self, request: dict, addr: str) -> None:
        method   = request["method"]
        func     = getattr(self, "rpc_%s" % method)
        logger.debug("Received %s RPC request from %s: %s", method, addr, request)

        # Make sure the method exists and is callable.
        if func is None or not callable(func):
            logger.warning("Received invalid RPC method from %s: %s", addr, method)
            return

        try:
            result = await func(addr, *request["args"])
            response = self.create_response(request["id"], result, None)
        except Exception as e:
            logger.exception("Error handling RPC request from %s: %s", addr, method)
            response = self.create_response(request["id"], None, e)

        text = json.dumps(response)
        data = text.encode("utf-8")
        self.transport.sendto(data, addr)

    def handle_response(self, response: dict, addr: str) -> None:
        '''
        Handle a response from a RPC call we made
        Params: response (dict)
        Returns: None
        '''
        logger.debug("Received response from %s: %s", addr, response)
        msg_id = response["id"]
        if msg_id not in self.outstanding:
            logger.warning("Received response for unknown request: %s", msg_id)
            return

        future, timeout = self.outstanding.pop(msg_id)
        timeout.cancel()

        future.set_result((response["result"], None))

    def create_request(self, method, *args, **kwargs) -> dict:
        return {
            "type"   : "request",
            "id"     : getrandbits(64),
            "method" : method,
            "args"   : list(args),
            "kwargs" : kwargs,
        }

    def _on_timeout(self, id, sender_id, method, addr) -> None:
        logger.error("Request %d from %s to %s for %s timed out.", id, sender_id, addr, method)
        # logger.error("Request %d timed out.", id)
        self.outstanding[id][0].set_result((None, asyncio.TimeoutError()))
        future, _ = self.outstanding.pop(id)

    def call(self, addr, method, *args, **kwargs) -> asyncio.Future:
        # Construct the RPC JSON message.
        request = self.create_request(method, *args, **kwargs)
        # logger.debug("Sending RPC request to %s: %s", addr, request)
        text    = json.dumps(request)
        data    = text.encode("utf-8")
        self.transport.sendto(data, addr)

        # Schedule a timeout for the request.
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        timer = loop.call_later(
            self.REQUEST_TIMEOUT,
            self._on_timeout,
            request["id"],
            request["args"][0],
            method,
            addr
        )

        # Store the future for later so we can set the result of it. Keep the
        # timer so we can cancel it if a response comes back.
        self.outstanding[request.get("id")] = (future, timer)
        return future

    def create_response(self, id, result, error) -> dict:
        return {
            "type": "result",
            "id": id,
            "result": result,
            "error": None if not error else {
                "type": type(error).__name__,
                "args": error.args
            }
        }

    # --------------------------------------------------------------------------
    # RPC Functions
    # --------------------------------------------------------------------------

    async def rpc_ping(self, addr: tuple, sender_id: int) -> int:
        '''
        Handle a ping request. 
        Params: address (tuple), id (int)
        Returns: id (int)
        '''
        # logger.debug("Received ping request from %s", sender_id)
        contact = Node(sender_id, *addr)
        self.client.table.greet(contact)

        return self.client.node.id

    async def rpc_find_node(self, addr: tuple, sender_id: int, target: int) -> list:
        '''
        Find the closest nodes to the given target.
        Params: address (tuple), id (int), target (int)
        Returns: list of (id, ip, port)
        '''
        contact = Node(sender_id, *addr)
        self.client.table.greet(contact)

        # Find the closest nodes to the target (excluding the sender)
        nodes = self.client.table.find_kclosest(target, self.client.k, exclude=contact)
        # logger.debug("Received find_node request from (%s, %s) for %s with result: %s, with exclude: %s", sender_id, addr, target, nodes, contact)
        return [
            (node.id, node.ip, node.port)
            for node in nodes
        ]

    async def rpc_find_value(self, address, sender_id, key) -> dict:
        # logger.debug("Received find_value request from %s for %s", sender_id, key)
        contact = Node(sender_id, *address)
        self.client.table.greet(contact)
        
        # If the node does not have the file, return the closest nodes.
        if not (doc := self.client.storage.get(key)):
            logger.debug("Node does not have file %s", key)
            logger.debug("Storage: %s", self.client.storage)
            return await self.rpc_find_node(address, sender_id, key)
        
        logger.debug("Node has file %s", key)
        return doc.dict
    
    async def rpc_store(self, address: tuple, sender_id: int, key: int, new_doc: dict) -> None:
        # logger.debug("Received store request from %s for %s:%s", sender_id, key, new_doc)
        sender = Node(sender_id, *address)
        self.client.table.greet(sender)

        # If there is no song with the given key, then add it
        if not (curr_doc := self.client.storage.get(key)):
            self.client.storage.add(key, KademilaFile(new_doc))
            return

        curr_version = curr_doc.version
        # If the doc is newer, update it (if there is a tie then compare by ID)
        if new_doc["version"] >= curr_version:
            if new_doc["version"] == curr_version:
                if new_doc["id"] < sender_id:
                    # If the incoming ID is smaller, then we keep the old doc
                    return
            # Otherwise, we update our local storage
            self.client.storage.add(key, KademilaFile(new_doc))

    async def make_call(self, node: Node, method, *args, **kwargs):
        '''
        Make a RPC call to a node and return the result.
        Params: node (Node), method (str), *args, **kwargs
        Returns: result (dict), error (str)
        '''
        addr = (node.ip, node.port)
        logger.debug("Making %s call to %s (%s): args[%s]", method, node, addr, args)
        result = await self.call(addr, method, self.client.node.id, *args, **kwargs)

        # TODO: Error, right now it captures general errors and timeouts in same way
        if result[1]:
            logger.warning("Error calling %s on %s: %s", method, node, result[1])
            self.client.table.remove_node(node)
            return result
      
        # If the node is new, greet it
        self.client.table.greet(node)
        return result

    async def direct_call(self, addr, method, *args, **kwargs):
        '''
        Make a direct RPC call to an address instead of a node.
        Params: addr (tuple), method (str), *args, **kwargs
        Returns: result (dict), error (str)
        '''
        logger.debug("Making a direct %s call to %s: args[%s]", method, addr, args)
        result = await self.call(addr, method, self.client.node.id, *args, **kwargs)
        return result

    def __getattr__(self, method):
        '''
        Dynamically create a method for each RPC function.
        Params: method (str)
        Returns: function
        
        Example:
        protocol.find_node(node_to_ask, target_node) -> find_node RPC
        '''
        if method.startswith("direct_"):
            return lambda addr, *args, **kwargs: self.direct_call(addr, method[7:], *args, **kwargs)
        return lambda node, *args, **kwargs: self.make_call(node, method, *args, **kwargs)