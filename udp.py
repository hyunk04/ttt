"""
UDP message protocol

Server -> Client:
{
    "id": str, uuid for match
    "turn": bool, indicates if it's player's turn
    "res": int, -3 waiting, -2 ongoing, -1 lost, 0 draw, 1 won
    "data": list[int], [-1, -1, -1, 0, 1, -1, 0, 1, -1], board state
}

Client -> Server:
{
    "id": str, uuid for match
    "data": int, representing client's move
            -2 for clean message
            -1 for the init message
            0 - 8 for valid move
}
"""

import socket
import threading
import queue
import json
import logging
from concurrent.futures import ThreadPoolExecutor


def setup_game_logger():
    logger = logging.getLogger("TicTacToe_UDP")
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()  # prints to console
    console_handler.setLevel(logging.DEBUG)
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%H:%M:%S"
    formatter = logging.Formatter(log_format, date_format)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logging.addLevelName(21, 'CONNECTION')
    logging.addLevelName(22, 'GAME')
    logging.addLevelName(23, 'MESSAGE')

    return logger


logger = setup_game_logger()


def log_connection(message):
    logger.log(21, message)


def log_game(message):
    logger.log(22, message)


def log_message(message):
    logger.log(23, message)


class UDPService:
    def __init__(self, host_ip: str, recv_port: int, send_port: int, message_buffer_size: int, buffer_in: queue.Queue,
                 buffer_out: queue.Queue):
        self.host_ip = host_ip
        self.recv_port = recv_port
        self.send_port = send_port
        self.buffer_in = buffer_in
        self.buffer_out = buffer_out
        self.message_buffer_size = message_buffer_size
        # recv socket
        self.socket_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_recv.bind((self.host_ip, self.recv_port))
        # send socket
        self.socket_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_send.bind((self.host_ip, self.send_port))
        # worker threads
        self.thread_recv = threading.Thread(target=self.recv_message)
        self.thread_send = threading.Thread(target=self.send_message)

    def recv_message(self):
        log_connection(f"UDP receive service started on {self.host_ip}:{self.recv_port}")
        while True:
            try:
                message_bytes, client_address = self.socket_recv.recvfrom(self.message_buffer_size)
                message_json = json.loads(message_bytes)
                log_message(f"Received {message_json} from {client_address}")
                self.buffer_in.put(
                    {
                        "client": client_address,
                        "message": message_json
                    }
                )
            except OSError as e:
                logger.error(f"Failed to receive message, socket closed: {e}")
                break
        log_connection("UDP receive service stopped")

    def send_message(self):
        log_connection(f"UDP send service started on {self.host_ip}:{self.send_port}")
        while True:
            try:
                message_json = self.buffer_out.get()
                if message_json is None:
                    break
                client_address = message_json["client"]
                message_bytes = json.dumps(message_json["message"]).encode("utf8")
                log_message(f"Sending message to {client_address}: {message_json['message']}")
                self.socket_send.sendto(message_bytes, client_address)
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                break
        log_connection("UDP send service stopped")

    def start(self):
        log_connection("Starting UDP service")
        self.thread_send.start()
        self.thread_recv.start()

    def stop(self):
        log_connection("Stopping UDP service")
        self.buffer_out.put(None)
        self.socket_recv.close()
        self.socket_send.close()
        self.thread_send.join()
        self.thread_recv.join()
        log_connection("UDP service stopped")


class SessionManager:
    def __init__(self, buffer_in: queue.Queue, buffer_out: queue.Queue, worker_pool_size: int) -> None:
        self.buffer_in = buffer_in
        self.buffer_out = buffer_out
        self.worker_pool_size = worker_pool_size
        self.worker_pool = ThreadPoolExecutor(max_workers=self.worker_pool_size)
        self.sessions = {}
        self.lock_sessions = threading.Lock()

    def is_valid_move(self, move: int, data: list[int]) -> bool:
        return 0 <= move < 9 and data[move] == -1

    def is_win_state(self, data: list[int]) -> bool:
        end_state = [[0, 1, 2], [3, 4, 5], [6, 7, 8],
                     [0, 3, 6], [1, 4, 7], [2, 5, 8],
                     [0, 4, 8], [2, 4, 6]]
        for i, j, k in end_state:
            if data[i] != -1 and data[i] == data[j] == data[k]:
                return True
        return False

    def is_draw_state(self, data: list[int]) -> bool:
        return -1 not in data

    def to_buffer_out(self, client_address: tuple, session_id: str, turn: bool, res: int, game_data: list[int]) -> None:
        message_json = {
            "client": client_address,
            "message": {
                "id": session_id,
                "turn": turn,
                "res": res,
                "data": game_data
            }
        }
        self.buffer_out.put(message_json)

    def manage_session(self):
        # No individual worker start message here
        while True:
            message_in = self.buffer_in.get()
            if message_in is None:
                break
            # retrieve data
            client_address = message_in["client"]  # tuple
            client_move = message_in["message"]["data"]  # int
            session_id = message_in["message"]["id"]  # str
            # acquire lock
            with self.lock_sessions:
                session = None if session_id not in self.sessions else self.sessions[session_id]
                # ----------------------- clean message -----------------------
                # -2 indicates that the match is over due to opponent disconnecting TCP socket unexpectedly
                if client_move == -2:
                    if session is not None:
                        log_game(f"Cleaning up session {session_id}")
                        self.sessions.pop(session_id)
                    continue
                # ----------------------- if session does not exist -----------------------
                if session is None:
                    log_game(f"Creating new session {session_id} for client {client_address}")
                    self.sessions[session_id] = {
                        "id": session_id,
                        "clients": [client_address],
                        "turn": 0,
                        "data": [-1 for _ in range(9)]
                    }
                    # commit outbound message, -3 for waiting for opponent state
                    self.to_buffer_out(client_address, session_id, True, -3, self.sessions[session_id]["data"])
                    continue
                # ----------------------- if session exists but the second player is missing -----------------------
                if len(session["clients"]) == 1 and client_address != session["clients"][0]:
                    log_game(f"Second player {client_address} joined session {session_id}")
                    session["clients"].append(client_address)
                    self.to_buffer_out(client_address, session_id, False, -3, session["data"])
                    # commit outbound message to both clients, client 0 always has the first move, -2 for ongoing match
                    log_game(f"Starting match for session {session_id}")
                    self.to_buffer_out(session["clients"][0], session_id, True, -2, session["data"])
                    self.to_buffer_out(session["clients"][1], session_id, False, -2, session["data"])
                    continue
                # ----------------------- if session exists and both players are present -----------------------
                current_player = session["turn"]  # gets the current player, which equals "turn" and index of "clients"
                # ignores update if the same player tries to update the session again, or if the player move is invalid, player loses
                if client_address != session["clients"][current_player] or not self.is_valid_move(client_move,
                                                                                                  session["data"]):
                    log_game(f"Invalid move in session {session_id}: player={current_player}, move={client_move}")
                    self.to_buffer_out(session["clients"][current_player], session_id, False, -1, session["data"])
                    self.to_buffer_out(session["clients"][(current_player + 1) % 2], session_id, False, 1,
                                       session["data"])
                    # delete session
                    log_game(f"Session {session_id} ended due to invalid move")
                    self.sessions.pop(session_id)
                    continue
                # if both player and move are valid, update player move
                session["data"][client_move] = current_player
                log_game(f"Valid move in session {session_id}: player={current_player}, move={client_move}")
                
                # check for session termination [start] -----------------------
                if self.is_win_state(session["data"]):
                    log_game(f"Player {current_player} won in session {session_id}")
                    # commit outbound message
                    self.to_buffer_out(session["clients"][current_player], session_id, False, 1, session["data"])
                    self.to_buffer_out(session["clients"][(current_player + 1) % 2], session_id, False, -1,
                                       session["data"])
                    # delete session
                    log_game(f"Session {session_id} ended with winner")
                    self.sessions.pop(session_id)
                    continue
                if self.is_draw_state(session["data"]):
                    log_game(f"Draw in session {session_id}")
                    # commit outbound message
                    self.to_buffer_out(session["clients"][current_player], session_id, False, 0, session["data"])
                    self.to_buffer_out(session["clients"][(current_player + 1) % 2], session_id, False, 0,
                                       session["data"])
                    # delete session
                    log_game(f"Session {session_id} ended with draw")
                    self.sessions.pop(session_id)
                    continue
                # check for session termination [end] -----------------------
                # if session not terminated, switch turns
                session["turn"] = (current_player + 1) % 2
                log_game(f"Turn changed in session {session_id}: new turn={session['turn']}")
                # commit outbound message
                self.to_buffer_out(session["clients"][current_player], session_id, False, -2, session["data"])
                self.to_buffer_out(session["clients"][session["turn"]], session_id, True, -2, session["data"])
        # No individual worker stop message here

    def start(self):
        log_connection(f"Starting session manager pool with {self.worker_pool_size} workers")
        for i in range(self.worker_pool_size):
            self.worker_pool.submit(self.manage_session)

    def stop(self):
        log_connection("Stopping session manager pool")
        for i in range(self.worker_pool_size):
            self.buffer_in.put(None)
        self.worker_pool.shutdown(wait=True)
        log_connection("Session manager pool stopped")


class UDPServer:
    def __init__(self, host_id: str, recv_port: int, send_port: int, message_buffer_size: int,
                 session_manager_worker_pool_size: int):
        self.host_id = host_id
        self.recv_port = recv_port
        self.send_port = send_port
        self.message_buffer_size = message_buffer_size
        self.session_manager_worker_pool_size = session_manager_worker_pool_size
        self.buffer_in = queue.Queue()
        self.buffer_out = queue.Queue()
        self.udp_service = UDPService(self.host_id, self.recv_port, self.send_port, self.message_buffer_size,
                                      self.buffer_in, self.buffer_out)
        self.session_manager = SessionManager(self.buffer_in, self.buffer_out, self.session_manager_worker_pool_size)

    def start(self):
        self.session_manager.start()
        self.udp_service.start()
        log_connection("UDP server fully started")

    def stop(self):
        self.udp_service.stop()
        self.session_manager.stop()
        log_connection("UDP server fully stopped")


SERVER_UDP_IP = "127.0.0.1"
SERVER_UDP_RECV_PORT = 55501
SERVER_UDP_SEND_PORT = 55502
MESSAGE_BUFFER_SIZE = 1024
SESSION_MANAGER_WORKER_POOL_SIZE = 8

if __name__ == '__main__':
    log_connection("Starting Tic Tac Toe UDP server")
    udp_server = UDPServer(SERVER_UDP_IP, SERVER_UDP_RECV_PORT, SERVER_UDP_SEND_PORT, MESSAGE_BUFFER_SIZE,
                           SESSION_MANAGER_WORKER_POOL_SIZE)
    udp_server.start()
    try:
        log_connection("UDP server running. Press Ctrl+C to stop.")
        while True:
            threading.Event().wait()
    except KeyboardInterrupt:
        log_connection("Keyboard interrupt received, shutting down UDP server")
        udp_server.stop()