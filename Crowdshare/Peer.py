from __future__    import annotations
from hashlib       import sha1
from Crowdshare.Node          import Node
from Crowdshare.Protocol       import RoutingTable
from Crowdshare.Protocol      import CrowdshareProtocol
from Crowdshare.Crawler       import Crawler
from Crowdshare.Kademila       import KademilaFile

from random        import getrandbits
from typing        import Union
from time          import monotonic_ns

import json
import asyncio
import logging
import pathlib
import os
import socket

logger             = logging.getLogger(__name__)
ALPHA              = 3
PREFIX_LEN         = 16
REPUBLISH_INTERVAL = 3600
REFRESH_INTERVAL   = 3600


class SongStorage:
    '''
    Storage class for kad-files
    '''
    def __init__(self):
        self.data = {}
    
    def add(self, song_key: int, kad_file: KademilaFile):
        '''
        Add a kad-file to the storage with a timestamp
        Params: song_id (int), kad_file (KademilaFile)
        Returns: None
        '''
        self.data[song_key] = (kad_file, monotonic_ns())
    
    def get(self, song_key: int) -> Union[KademilaFile, None]:
        '''
        Retreive a kad-file from the storage.
        Params: song_id (int)
        Returns: kad_file (KademilaFile)
        '''
        if song_key in self.data:
            return self.data[song_key][0]
        return None
    
    def get_time(self, song_key: int) -> Union[int, None]:
        '''
        Retreive the timestamp of a kad-file from the storage.
        Params: song_id (int)
        Returns: timestamp (int)
        '''
        if song_key in self.data:
            return self.data[song_key][1]
        return None
    
    def get_republish_list(self, refresh_time: int) -> list[tuple[int, KademilaFile]]:
        '''
        Returns a list of (song_key: kad-files) that are older than refresh_time

        # Sec 2.5 optimization
        '''
        min_time = monotonic_ns() - (refresh_time * 10**9)
        for song_key, (kad_file, timestamp) in self.data.items():
            if timestamp < min_time:
                yield song_key, kad_file
    
    def __repr__(self):
        string = ['SongStorage:\n']
        for song_key, (kad_file, timestamp) in self.data.items():
            if len(str(song_key)) > 10:
                song_key = str(song_key)[:10] + '...'
            string.append(f"{song_key}: {kad_file}")
        return ''.join(string)
        


class Peer:
    def __init__(self, _id = None, k: int = 20, kad_path: str = "kad_files"):
        self.node          = Node(_id=_id)
        self.k             = k
        self.alpha         = 3   
        self.protocol      = self._create_factory()
        self.table         = RoutingTable(self.node.id, k, self.protocol)
        self.kad_path      = pathlib.Path(__file__).parent.absolute() / kad_path
        self.storage       = SongStorage()
    
    async def get(self, song_name: str, artist_name: str) -> Union[KademilaFile, None]:
        '''
        Gets the max version kad-file from the network from a given song ID.
        Params: key (str)
        Returns: kad-files (dict[dict])
        '''

        # Creating the song_id "song_name - artist_name"
        song_id = f"{song_name}-{artist_name}"
        logger.info(f'Getting {song_id} from the network')

        key = int(sha1(song_id.encode()).hexdigest(), 16)

        song_node = Node(_id=key)
        if not (closest_nodes := self.table.find_kclosest(song_node.id, self.k)):
            logger.warning('Could not find any nodes close to song ID: %s', song_id)
            return None
        
        logger.debug("Starting to crawl network for kad-file %s with closest_nodes: %s", song_id, closest_nodes)
        # Crawl the network to find the kad-files
        crawler = Crawler(self.protocol, song_node, closest_nodes, self.k, self.alpha, "find_value")

        # crawler.lookup() should return the kad_file with the highest version
        kad_file = await crawler.lookup()
        if not kad_file:
            logger.debug("Could not find kad-file for %s", song_id)
            return None
        
        # Create a co-routine to download the file from the kad_file providers and return if it was successful
        if await self._download_file(kad_file):
            logger.info(f'Successfully downloaded {song_id} in {crawler.num_lookups} lookups')
            print(f'Successfully downloaded {song_id} in {crawler.num_lookups} lookups')
            kad_file.add_provider(self)
            self.storage.add(key, kad_file)

            # Send STORE calls to the k closest nodes to the song id to let them know we have the file (ensure future but do not wait for them)
            kclosest_nodes = list(crawler.closest)
            for node in kclosest_nodes:
                asyncio.create_task(self.protocol.store(node, key, kad_file.dict))
            return kad_file
        else:
            logger.warning(f'Failed to download {song_id}')
            return None

        
    async def _download_file(self, kad_file: KademilaFile) -> bool:
        '''
        Downloads the file from the kad_file providers
        Params: kad_file (KademilaFile)
        Returns: success (bool)
        '''
        downloaded_file = False
        request         = json.dumps({"song_id": kad_file.song_id})
        prefix          = f"{len(request)}".zfill(PREFIX_LEN)
        message 	    = f"{prefix}{request}".encode("utf-8")

        logger.debug("Providers for file: %s", kad_file.providers)
        for provider in kad_file.providers:
            node_id, addr = provider 
            try:
                logger.debug("In _download_file, connecting to %s", addr)

                # Connect to the provider
                reader, writer = await asyncio.open_connection(*addr)	
                writer.write(message)
                await writer.drain()

                # Read the prefix from the provider
                prefix     = await reader.readexactly(PREFIX_LEN)
                prefix     = prefix.decode("utf-8")
                size       = int(prefix)
                logger.debug("Successfully read the prefix from the provider, size: %s", size)

                # Read exactly size # of bytes
                song_data = await reader.readexactly(size)

                # song_data = await reader.readexactly(size)
                logger.debug("Successfully read the file from the provider of size: %s", len(song_data))
                writer.close()
                await writer.wait_closed()
                downloaded_file = True
                break
            except Exception as e:
                logger.warning('Could not connect to provider for downloading %s: %s', node_id, e)
                raise e
                self.table.remove_node(Node(node_id))
        
        if not downloaded_file:
            logger.warning('Could not download file from any provider')
            return False

        # TODO: Put this somewhere else. Create KAD_DIR if it does not exist
        if not os.path.exists(self.kad_path):
            os.makedirs(self.kad_path)
        
        file_path = pathlib.Path(self.kad_path, f"{kad_file.song_id}.kad")
        with open(file_path, 'wb') as f:
            f.write(song_data)
        return True
    
        
    async def put(self, song_name, artist_name) -> bool:
        '''
        Upload a kad-file to the network.
        Params: song_name (str), artist_name (str)
        Returns: success (bool)
        '''
        song_id = f"{song_name}-{artist_name}"
        logger.info(f'Putting {song_id} on the network')
        key       = int(sha1(song_id.encode()).hexdigest(), 16)
        song_node = Node(_id=key)

        if not (result := await self._construct_kad_file(song_name, artist_name, song_node)):
            return False
        kad_file, closest_nodes = result

        # Store the kad-file in the local storage
        self.storage.add(key, kad_file)

        # Send a STORE call to each of the k closest nodes
        futures = [self.protocol.store(node, key, kad_file.dict) for node in closest_nodes]
         
        # Return true if any of the STORE calls return true
        return any(await asyncio.gather(*futures))

    
    async def _construct_kad_file(self, song_name: str, artist_name: str, song_node: Node) -> KademilaFile: 
        '''
        Given a song name and artist name, constructs a new kad-file.
        Params: song_name (str), artist_name (str)
        Returns: KademilaFile
        '''
        # Get k closest neighbors
        if not (closest_nodes := self.table.find_kclosest(song_node.id, self.k)):
            logger.warning('Could not find any nodes close to song ID: %s', song_node.id)
            return None

        crawler  = Crawler(self.protocol, song_node, closest_nodes, self.k, self.alpha, "find_value")
        # First, try to find the largest version # of the kad file in the network (if exists)
        kad_file = await crawler.lookup()
        max_version = 0 if not kad_file else kad_file.version

        print(f"Constructing Kad_File: {crawler.num_lookups} lookups")
        logger.debug("Constructing Kad File - addr (%s:%s)", self.node.ip, self.node.port)
        return KademilaFile({
            'version'    : max_version + 1,
            'song_name'  : song_name,
            'artist_name': artist_name,
            'providers'  : [(self.node.id, (self.node.ip, self.node.port))],
            'id'         : getrandbits(160)
        }), crawler.closest
        

    def _create_factory(self):
        '''
        Creates a factory for the node to listen for incoming
        requests.
        Params: None
        Returns: twisted.internet.protocol.Factory
        '''
        return CrowdshareProtocol(self)

    async def listen(self, port: int, ip: str):
        self.node.ip, self.node.port = ip, port
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(self._create_factory, local_addr=(ip, port))
        logger.info(f'Listening on {self.node.ip}:{self.node.port}')
        self.transport, self.protocol = await listen
        self._schedule_refresh()
        asyncio.ensure_future(self._init_server())

    async def _init_server(self):
        server = await asyncio.start_server(self._server_handler, self.node.ip, self.node.port)
        logger.debug("In _init_server, server started on %s:%s", self.node.ip, self.node.port)
        async with server:
            await server.serve_forever()

    async def _server_handler(self, reader, writer):
        sender_addr = writer.get_extra_info('peername')
  
        # Read prefix len of message from socket
        prefix  = await reader.read(PREFIX_LEN)
        prefix  = prefix.decode()
        size    = int(prefix)

        logger.debug("In send_file, read prefix %s from %s", prefix, sender_addr)
  
        # Read request from socket
        data      = await reader.readexactly(size)
        data      = data.decode()
        request   = json.loads(data)
        song_id   = request['song_id']
        song_path = pathlib.Path(self.kad_path) / f"{song_id}.kad"

        logger.debug("In send_file, read request %s from %s", request, sender_addr)

        # Read the file from the kad_files directory
        with open(song_path, 'rb') as f:
            buffer = f.read()

        size    = len(buffer)
        prefix  = f"{size}".zfill(PREFIX_LEN).encode("utf-8")
        message = prefix + buffer

        logger.debug("In send_file, sending file of size %s", size)

        # Send the file to the requesting node
        writer.write(message)
        logger.debug("In send_file, successfully wrote to %s", sender_addr)
        await writer.drain()
        await writer.wait_closed()
    
    def _schedule_refresh(self):
        '''
        Schedules the routing table to be refreshed every hour.
        '''
        asyncio.ensure_future(self._refresh_table())
        loop = asyncio.get_event_loop()
        self.refresh = loop.call_later(REFRESH_INTERVAL, self._schedule_refresh)
    
    async def _refresh_table(self):
        '''
        Refresh the buckets in the routing table that haven't had any lookups in 1 hour.
        '''
        tasks = []
        for node_id in self.table.refresh_list:
            node_to_find = Node(_id=node_id)
            kclosest     = self.table.find_kclosest(node_id, limit=ALPHA)
            crawler      = Crawler(self.protocol, node_to_find, kclosest, self.k, self.alpha, "find_node")
            tasks.append(crawler.lookup())

        # Crawl the network for each node in the refresh list
        await asyncio.gather(*tasks)

        # Republish all kad files to the network
        for key, kad_file in self.storage.get_republish_list(REPUBLISH_INTERVAL):
            # Ensure this future
            asyncio.ensure_future(self._republish(key, kad_file))
    
    async def _republish(self, song_key: str, kad_file: KademilaFile):
        '''
        Helper function to republish an exisitng kad file to the network.
        Params: song_key (str) 
        Returns: None
        '''
        # Get k closest neighbors
        if not (closest_nodes := self.table.find_kclosest(song_key, self.k)):
            logger.warning('Could not find any nodes close to song ID for republishing: (%s, %s)', song_key, kad_file)
            return
        
        # Crawl network to find k closest nodes
        target_node = Node(_id=song_key)
        crawler  = Crawler(self.protocol, target_node, closest_nodes, self.k, self.alpha, "find_node")
        kclosest = await crawler.lookup()

        # Send store to kclosest
        tasks = []
        for node in kclosest:
            tasks.append(self.protocol.store(node, song_key, kad_file.dict))
        await asyncio.gather(*tasks)

        
    
    async def _bootstrap_node(self, addr: tuple[str, int]):
        '''
        Bootstraps the node to the network.
        Params: host - IP address of the bootstrap node
                port - Port of the bootstrap node
        Returns: Node or None
        '''
        # Send a ping request, which will return the node's ID
        logger.debug(f'Bootstrapping to {addr}...')
        result = await self.protocol.direct_ping(addr)

        # Ping was unsuccessful
        if result[1]:
            return None

        # If the ping was successful, add the node to the routing table
        bootstrap = Node(_id=result[0], ip=addr[0], port=addr[1])
        self.table.add_node(bootstrap)
        return bootstrap
            

    async def bootstrap(self, bootstraps: list[tuple[str, int]] = []):
        if not bootstraps:
            return
        
        # Try to bootstrap to each node
        tasks = map(self._bootstrap_node, bootstraps)
        results = await asyncio.gather(*tasks)

        # Only keep the nodes that were successfully bootstrapped to
        bootstraps = [task for task in results if task]
        
        network_crawler = Crawler(self.protocol, self.node, bootstraps, self.k, self.alpha, "find_node")
        await network_crawler.lookup()
        print(f"Boostrapped in {network_crawler.num_lookups} lookups")
    
    async def find_contact(self, target_id: int, k: int):
        target = Node(_id=target_id)

        kclosest = self.table.find_kclosest(target_id, limit=self.k, exclude=self.node)
        logger.debug(f'In find_contact, kclosest neighbors: {kclosest}')
        crawler = Crawler(self.protocol, target, kclosest, k, self.alpha, "find_node")
        result = await crawler.lookup()

        for node in result:
            if node.id == target_id:
                return (True, result)
        return (False, result)

    
    def get_info(self):
        return (self.node.ip, self.node.port, self.node.id)

    def __repr__(self):
        return str(self.node.id)
