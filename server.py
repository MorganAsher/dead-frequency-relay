"""Room directory and WebSocket relay for Dead Frequency."""

import argparse
import asyncio
import json
import secrets
import ssl
import struct
import time
from dataclasses import dataclass, field
from http import HTTPStatus

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

VERSION = 1
ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 6
MAX_PLAYERS = 4
HEADER = struct.Struct("<iBB")
MAX_FRAME = 8192 + HEADER.size
RELIABLE = 2


@dataclass
class Player:
    socket: ServerConnection
    peer_id: int
    outgoing: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=96))
    closing: bool = False

    def send(self, message: str | bytes, reliable: bool = True) -> None:
        if self.closing:
            return
        if not self.outgoing.full():
            self.outgoing.put_nowait(message)
        elif reliable:
            self.closing = True
            asyncio.create_task(self.socket.close(1013, "Connection too slow."))

    def control(self, op: str, **values) -> None:
        self.send(json.dumps({"op": op, **values}, separators=(",", ":")))

    async def write(self) -> None:
        while True:
            await self.socket.send(await self.outgoing.get())


@dataclass
class Room:
    code: str
    players: dict[int, Player]
    dedicated: bool = False
    name: str = ""
    protocol: int = 0
    next_peer: int = 2
    locked: bool = False

    @property
    def player_count(self) -> int:
        return len(self.players) - int(self.dedicated)

    def forward(self, player: Player, frame: bytes) -> None:
        if not HEADER.size < len(frame) <= MAX_FRAME:
            return
        target, mode, channel = HEADER.unpack_from(frame)
        if mode > RELIABLE or channel > 4:
            return

        # The relay assigns the sender ID. Players can only talk to their host.
        message = HEADER.pack(player.peer_id, mode, channel) + frame[HEADER.size:]
        if player.peer_id != 1:
            if target == 1 and 1 in self.players:
                self.players[1].send(message, mode == RELIABLE)
        elif target > 1:
            if target in self.players:
                self.players[target].send(message, mode == RELIABLE)
        elif target <= 0:
            for peer_id, recipient in self.players.items():
                if peer_id not in (1, -target):
                    recipient.send(message, mode == RELIABLE)


class RoomDirectory:
    def __init__(self, max_rooms: int) -> None:
        self.rooms: dict[str, Room] = {}
        self.max_rooms = max_rooms
        self.connections = 0

    def route(self, connection, request):
        if request.path == "/health":
            return connection.respond(HTTPStatus.OK, "ok\n")
        if request.path == "/servers":
            servers = [
                {"code": room.code, "name": room.name, "players": room.player_count,
                 "capacity": MAX_PLAYERS, "locked": room.locked, "protocol": room.protocol}
                for room in self.rooms.values() if room.name
            ]
            servers.sort(key=lambda entry: (
                entry["locked"] or entry["players"] >= entry["capacity"], entry["name"].casefold()
            ))
            response = connection.respond(HTTPStatus.OK, json.dumps({"servers": servers}, ensure_ascii=False))
            del response.headers["Content-Type"]
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            response.headers["Cache-Control"] = "no-store"
            return response
        if request.path != "/room":
            return connection.respond(HTTPStatus.NOT_FOUND, "Not found\n")
        return None

    def admit(self, socket: ServerConnection, request: dict) -> tuple[Room, Player]:
        if request.get("version") != VERSION:
            raise ValueError("This relay needs a different game build.")
        op = request.get("op")
        if op == "create":
            if len(self.rooms) >= self.max_rooms:
                raise ValueError("The relay is busy. Try again shortly.")
            dedicated = request.get("dedicated") is True
            name = request.get("name", "") if dedicated else ""
            protocol = request.get("protocol", 0) if dedicated else 0
            if not isinstance(name, str) or (name and (
                len(name) > 40 or not name.isprintable() or name != name.strip()
            )):
                raise ValueError("Use a server name of 1 to 40 printable characters.")
            if type(protocol) is not int or not 0 <= protocol <= 65535:
                raise ValueError("Invalid game protocol.")
            code = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LENGTH))
            while code in self.rooms:
                code = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LENGTH))
            player = Player(socket, 1)
            room = Room(code, {1: player}, dedicated=dedicated, name=name, protocol=protocol)
            self.rooms[code] = room
        elif op == "join":
            code = request.get("code")
            room = self.rooms.get(code) if isinstance(code, str) else None
            if room is None:
                raise ValueError("Room code not found. Check the code with your host.")
            if room.locked:
                raise ValueError("This run has already started.")
            if room.player_count >= MAX_PLAYERS:
                raise ValueError("The lobby is full.")
            player = Player(socket, room.next_peer)
            room.next_peer += 1
            room.players[player.peer_id] = player
            room.players[1].control("peer_joined", id=player.peer_id)
        else:
            raise ValueError("Choose to host a room or join with a code.")
        player.control("room", id=player.peer_id, code=room.code, dedicated=room.dedicated)
        return room, player

    async def handle(self, socket: ServerConnection) -> None:
        if self.connections >= self.max_rooms * MAX_PLAYERS + 32:
            await socket.close(1013, "The relay is busy.")
            return
        self.connections += 1
        room = None
        player = None
        writer = None
        try:
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=5)
                request = json.loads(raw) if isinstance(raw, str) and len(raw) <= 256 else None
                if not isinstance(request, dict):
                    raise ValueError("Invalid room request.")
                room, player = self.admit(socket, request)
            except (ValueError, TimeoutError) as error:
                message = str(error) if type(error) is ValueError else "Invalid room request."
                await socket.send(json.dumps({"op": "error", "message": message}))
                await socket.close(1008, message)
                return

            writer = asyncio.create_task(player.write())
            window_start = time.monotonic()
            packets = 0
            bytes_received = 0
            async for message in socket:
                now = time.monotonic()
                if now - window_start >= 1:
                    window_start = now
                    packets = 0
                    bytes_received = 0
                packets += 1
                bytes_received += len(message)
                if packets > 1200 or bytes_received > 2_000_000:
                    await socket.close(1008, "Relay traffic limit exceeded.")
                    break
                if isinstance(message, bytes):
                    room.forward(player, message)
                elif player.peer_id == 1:
                    await self.host_control(room, message)
        except ConnectionClosed:
            pass
        finally:
            self.connections -= 1
            if writer is not None:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            if room is not None:
                await self.remove(room, player)

    async def remove(self, room: Room, player: Player) -> None:
        room.players.pop(player.peer_id, None)
        if player.peer_id == 1:
            self.rooms.pop(room.code, None)
            guests = list(room.players.values())
            room.players.clear()
            await asyncio.gather(*(
                guest.socket.close(1000, "The host closed the room.") for guest in guests
            ))
        elif 1 in room.players:
            room.players[1].control("peer_left", id=player.peer_id)

    async def host_control(self, room: Room, message: str) -> None:
        if message == "lock":
            room.locked = True
        elif room.dedicated and message == "unlock" and room.player_count == 0:
            room.locked = False
        elif room.dedicated and message.startswith("kick:") and len(message) <= 16:
            peer_id = message.removeprefix("kick:")
            if peer_id.isascii() and peer_id.isdecimal():
                peer = room.players.get(int(peer_id))
                if peer is not None and peer.peer_id != 1:
                    await peer.socket.close(1008, "The dedicated server did not accept the player.")


async def main(args) -> None:
    tls = None
    if args.cert:
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.load_cert_chain(args.cert, args.key)
    directory = RoomDirectory(args.max_rooms)
    async with serve(directory.handle, args.bind, args.port, ssl=tls, origins=[None],
                     process_request=directory.route, compression=None, max_size=MAX_FRAME,
                     max_queue=16, open_timeout=5, close_timeout=2, ping_interval=15, ping_timeout=10):
        scheme = "wss" if tls else "ws"
        print(f"Dead Frequency relay listening on {scheme}://{args.bind}:{args.port}/room", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--max-rooms", type=int, default=64)
    parser.add_argument("--cert", help="TLS certificate chain in PEM format")
    parser.add_argument("--key", help="TLS private key in PEM format")
    args = parser.parse_args()
    if bool(args.cert) != bool(args.key):
        parser.error("--cert and --key must be supplied together")
    if not 1 <= args.port <= 65535 or not 1 <= args.max_rooms <= 1024:
        parser.error("choose a valid port and between 1 and 1024 rooms")
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        pass
