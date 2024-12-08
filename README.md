# Crowdshare

Crowdshare is a decentralized peer-to-peer file-sharing network for uploading and downloading songs.

## Getting Started

### Running as the First Node

To start the first node in the network:

```bash
python3 client.py --ip=<ip> --port=<port>
```

This will start the interactive shell with the following commands:

- `get <SongName> <ArtistName>`: Download a song.
- `put <SongName> <ArtistName>`: Upload a song.
- `table`: View the routing table.
- `info`: Show peer's IP, port, and ID.

### Running as a Non-First Node

To join an existing network, specify bootstrap nodes:

```bash
python3 client.py --ip=<ip> --port=<port> --b=<bootstrap1:port1,bootstrap2:port2>
```

Replace `<bootstrap1:port1,bootstrap2:port2>` with the addresses of existing nodes.

## Uploading and Storing Files

1. **Create a `kad_files` directory** in the `p2play` directory:

   ```bash
   mkdir -p crowdshare/kad_files
   ```

2. **Move songs** into the `kad_files` directory and **rename them** to the format `SongName-ArtistName.kad` (e.g., `ShapeOfYou-EdSheeran.kad`).

3. **Upload the song** using:

   ```bash
   put SongName ArtistName
   ```

## Commands

- `get <SongName> <ArtistName>`: Download a song.
- `put <SongName> <ArtistName>`: Upload a song.
- `table`: View the routing table.
- `info`: Show peer's IP, port, and ID.


