import tkinter as tk
import socket
import threading
import queue
import json

import logging 


def game_logger_setup():
    logger = logging.getLogger("TicTacToe Game")
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


logger = game_logger_setup()


def log_connection(message):
    logger.log(21, message)


def log_game(message):
    logger.log(22, message)


def log_message(message):
    logger.log(23, message)


class TCPService:
    def __init__(self, client_ip: str, client_port: int, server_ip: str, server_port: int):
        self.socket_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_tcp.bind((client_ip, client_port))
        self.socket_tcp.connect((server_ip, server_port))
        self.buffer_in = queue.Queue()
        self.buffer_out = queue.Queue()
        self.thread_send = threading.Thread(target=self.send_message)
        self.thread_recv = threading.Thread(target=self.recv_message)
        self.shutdown = threading.Event()

    def send_message(self):
        while not self.shutdown.is_set():
            message = self.buffer_out.get()
            if message is None:
                self.shutdown.set()
                break
            try:
                self.socket_tcp.send(message.encode())
                log_message(f"sent TCP message {message}")
            except socket.error as e:
                logger.error(f"failed to send message {message}", e)
                self.shutdown.set()
                break
            except KeyboardInterrupt:
                log_connection("TCP send service received keyboard interrupt")
                self.shutdown.set()
                break

    def recv_message(self):
        while not self.shutdown.is_set():
            try:
                log_message("receiving TCP messages...")
                message_bytes = self.socket_tcp.recv(MESSAGE_BUFFER_SIZE)
                log_message(f"received TCP message {message_bytes.decode()}")
                if not message_bytes:
                    log_connection("TCP socket connection closed")
                    self.shutdown.set()
                    break
                self.buffer_in.put(message_bytes.decode())
                logger.error(f"put TCP message {message_bytes.decode()} into buffer")
            except socket.error as e:
                logger.error(f"failed to receive message from server", e)
                self.shutdown.set()
                break
            except KeyboardInterrupt:
                log_connection("TCP recv service received keyboard interrupt")
                self.shutdown.set()
                break

    def start(self):
        self.thread_send.start()
        self.thread_recv.start()
        log_connection("TCP service started")

    def get_message(self):
        return self.buffer_in.get()  # blocking

    def put_message(self, message: str):
        self.buffer_out.put(message)

    def stop(self):
        self.shutdown.set()
        self.buffer_out.put(None)
        self.thread_send.join()
        self.socket_tcp.close()
        self.thread_recv.join()
        self.buffer_in.put(None)
        log_connection("TCP service stopped")


class UDPService:
    def __init__(self, client_ip: str, client_port: int, server_ip: str, server_port: int):
        self.socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_udp.bind((client_ip, client_port))
        self.server_ip = server_ip
        self.server_port = server_port
        self.buffer_in = queue.Queue()
        self.buffer_out = queue.Queue()
        self.thread_send = threading.Thread(target=self.send_message)
        self.thread_recv = threading.Thread(target=self.recv_message)
        self.shutdown = threading.Event()

    def send_message(self):
        while not self.shutdown.is_set():
            message = self.buffer_out.get()
            if message is None:
                self.shutdown.set()
                break
            try:
                self.socket_udp.sendto(message.encode(), (self.server_ip, self.server_port))
                log_message(f"Sent UDP Message: {message}")
            except socket.error as e:
                logger.error(f"Failed to send message {message}: {e}")
                self.shutdown.set()
                break
            except KeyboardInterrupt:
                log_connection("UDP send service received keyboard interrupt")
                self.shutdown.set()
                break

    def recv_message(self):
        while not self.shutdown.is_set():
            try:
                message_bytes, addr = self.socket_udp.recvfrom(MESSAGE_BUFFER_SIZE)
                self.buffer_in.put(message_bytes.decode())
                log_message(f"received UDP message {message_bytes.decode()}")
            except socket.error as e:
                logger.error(f"Failed to receive message from server: {e}")
                self.shutdown.set()
                break
            except KeyboardInterrupt:
                log_connection("UDP recv service received keyboard interrupt")
                self.shutdown.set()
                break

    def start(self):
        self.thread_send.start()
        self.thread_recv.start()
        log_connection("UDP service started")

    def get_message(self):
        return self.buffer_in.get()  # blocking

    def put_message(self, message: str):
        self.buffer_out.put(message)

    def stop(self):
        self.shutdown.set()
        self.buffer_out.put(None)
        self.thread_send.join()
        self.socket_udp.close()
        self.thread_recv.join()
        self.buffer_in.put(None)
        log_connection("UDP service stopped")


class Client:
    def __init__(self, ip_tcp_server: str, port_tcp_server: int, ip_client: str, port_tcp_client: int,
                 port_udp_client: int):
        # Game state
        self.session_id = None
        self.turn = None  # False: opponent's turn, True: your turn
        self.player = None  # 0: X, 1:O
        self.board = None  # list[int]
        self.stats = None  # str
        self.match_over = True
        # GUI
        self.top_level_widget = tk.Tk()
        self.top_level_widget.title("Tic Tac Toe")
        self.top_level_widget.protocol("WM_DELETE_WINDOW", self.stop)
        self.canvas_game_board = self.create_canvas_game_board()
        self.widget_message_display = self.create_widget_message_display()
        self.entry_message_input = self.create_entry_message_input()
        self.create_button_game_state()
        self.draw_board()
        self.update_lock = threading.Lock()
        self.update_queue = queue.Queue()
        # Networking
        self.ip_tcp_server = ip_tcp_server
        self.port_tcp_server = port_tcp_server
        self.ip_client = ip_client
        self.port_tcp_client = port_tcp_client
        self.port_udp_client = port_udp_client
        self.tcp_service = None
        self.udp_service = None
        # Shutdown flag
        self.shutdown = threading.Event()
        # Update GUI
        self.thread_tcp_consumer = None
        self.thread_udp_consumer = None
        self.top_level_widget.after(10, self.process_gui_updates)

        log_connection(f"Client initialized with TCP server {ip_tcp_server}:{port_tcp_server}")

    def process_gui_updates(self):
        try:
            while True:
                update_type, *args = self.update_queue.get_nowait()
                if update_type == "message":
                    self.display_message(*args)
                elif update_type == "start_tcp":
                    self.display_message("[SERVER] Connecting to TCP server...")
                    self.thread_tcp_consumer = threading.Thread(target=self.consume_message_tcp)
                    self.thread_tcp_consumer.start()
                    self.tcp_service = TCPService(self.ip_client, self.port_tcp_client, self.ip_tcp_server,
                                                  self.port_tcp_server)
                    self.tcp_service.start()
                    self.display_message("[MATCH] Match making...")
                elif update_type == "restart":
                    if not self.match_over:
                        self.display_message("[ERROR] Current match is not over, cannot find new match...")
                        log_game("Restart rejeced: match in progress")
                    else:
                        log_game("Restarting game services and finding new match")
                        self.stop_services()
                        self.session_id = None
                        self.turn = None
                        self.player = None
                        self.board = None
                        self.stats = None
                        self.shutdown.clear()
                        self.update_queue.put(("start_tcp",))
                elif update_type == "start_udp":
                    self.display_message("[SERVER] Connecting to UDP server...")
                    self.thread_udp_consumer = threading.Thread(target=self.consume_message_udp)
                    self.thread_udp_consumer.start()
                    msg_json = args[0]
                    self.session_id = msg_json["data"]["id"]
                    ip_udp = msg_json["data"]["ip"]
                    port_udp = msg_json["data"]["port"]
                    log_connection(f"UDP connection to {ip_udp}:{port_udp} for session {self.session_id}")
                    self.udp_service = UDPService(self.ip_client, self.port_udp_client, ip_udp, port_udp)
                    self.udp_service.start()
                    self.udp_service.put_message(json.dumps({"id": self.session_id, "data": -1}))
                    self.match_over = False
                elif update_type == "init_match":
                    msg_json = args[0]
                    self.session_id = msg_json["id"]
                    self.player = 0 if msg_json["turn"] else 1
                    log_game(f"Match intialized: session {self.session_id}, player {self.player}")
                    self.display_message("[MATCH] Waiting for opponent")
                elif update_type == "update_match":
                    msg_json = args[0]
                    if self.board is None:
                        self.display_message("[MATCH] Opponent has joined")
                        log_game("Opponent joined the match")

                    self.turn = msg_json["turn"]

                    if self.turn:
                        turn_status = "[TURN] Your Turn!"

                    else:
                        turn_status = "[TURN] Opponents Turn!"

                    self.display_message(turn_status)
                    log_game(f"Turn update: {turn_status}")    
                    self.board = msg_json["data"]
                    log_game(f"Board state: {self.board} ")
                    self.draw_board()

                elif update_type == "end_match":
                    msg_json = args[0]
                    res = msg_json["res"]
                    if res == -1:
                        result_message = "[GAME] You lost :("
                        self.tcp_service.put_message(
                            json.dumps({"type": "control", "data": {"flag": "fin", "res": res}}))
                    elif res == 0:
                        result_message = "[GAME] Draw!"
                        self.tcp_service.put_message(
                            json.dumps({"type": "control", "data": {"flag": "fin", "res": res}}))
                    elif res == 1:
                        result_message = "[GAME] You Won! :)"
                        self.tcp_service.put_message(
                            json.dumps({"type": "control", "data": {"flag": "fin", "res": res}}))
                    log_game(f"Match ended: {result_message}")
                    self.board = msg_json["data"]
                    self.draw_board()
                    self.udp_service.put_message(json.dumps({"id": self.session_id, "data": -2}))
                    log_message("UDP Clean message sent")
                elif update_type == "stats":
                    msg_json = args[0]
                    stats_json = msg_json["data"]["res"]
                    
                    wins = stats_json.get("win", 0)
                    losses = stats_json.get("loss", 0)
                    draws = stats_json.get("draw", 0)
                    
                    stats_message = f"[STATS] Game Stats: {wins} Wins, {losses} Losses, {draws} Draws"
                    self.display_message(stats_message)
                    log_game(f"Stats received: {stats_json}")
                    self.match_over = True
                elif update_type == "exit":
                    log_connection("Exit Requested")
                    self.shutdown.set()
                    self.stop()

        except queue.Empty:
            pass
        self.top_level_widget.after(10, self.process_gui_updates)

    def create_canvas_game_board(self) -> tk.Canvas:
        canvas = tk.Canvas(self.top_level_widget, width=CELL_SIZE * BOARD_SIZE, height=CELL_SIZE * BOARD_SIZE)
        canvas.pack(pady=CANVAS_PADDING)
        canvas.bind("<Button-1>", self.on_click)
        return canvas

    def on_click(self, event):
        if self.turn:
            col = event.x // CELL_SIZE
            row = event.y // CELL_SIZE
            index = row * BOARD_SIZE + col
            if 0 <= row < BOARD_SIZE and 0 <= col < BOARD_SIZE and self.board[index] == -1:  # BOARD_SIZE = 3
                self.board[index] = self.player  # update board state
                log_game(f"Player made a move at position {index}")
                self.update_queue.put(("board", self.board.copy()))  # Sends a copy
                if self.udp_service:
                    self.udp_service.put_message(json.dumps({
                        "id": self.session_id,
                        "data": index
                    }))
            else:
                log_game(f"Invalid move attempted at position {index}")
                self.display_message("[ERROR] Invalid Move!")
        else:
            log_game("Move attempted when not player's turn")
            self.display_message("[ERROR] Not your Turn!")

    def draw_board(self):
        # Clear canvas
        self.canvas_game_board.delete("all")
        # Draw grid
        for i in range(1, BOARD_SIZE):
            self.canvas_game_board.create_line(i * CELL_SIZE, 0, i * CELL_SIZE, CELL_SIZE * BOARD_SIZE)
            self.canvas_game_board.create_line(0, i * CELL_SIZE, CELL_SIZE * BOARD_SIZE, i * CELL_SIZE)
        if self.board and len(self.board) == 9:
            # Draw symbols from board (1D list)
            for row in range(BOARD_SIZE):
                for col in range(BOARD_SIZE):
                    index = row * BOARD_SIZE + col
                    value = self.board[index]
                    symbol = "X" if value == 0 else "O" if value == 1 else None
                    color = "blue" if symbol == "X" else "red"
                    if symbol:
                        x_center = col * CELL_SIZE + CELL_SIZE // 2
                        y_center = row * CELL_SIZE + CELL_SIZE // 2
                        self.canvas_game_board.create_text(x_center, y_center, text=symbol, font=("Arial", 48, "bold"),
                                                           fill=color)

    def create_widget_message_display(self) -> tk.Text:
        frame_message_display = tk.Frame(self.top_level_widget)
        frame_message_display.pack(pady=FRAMES_PADDING, fill="both", expand=True)
        widget_text = tk.Text(frame_message_display, height=TEXT_WIDGET_HEIGHT, state="disabled", wrap="word")
        widget_text.pack(side="left", fill="both", expand=True)
        scrollbar_text = tk.Scrollbar(frame_message_display, command=widget_text.yview)
        scrollbar_text.pack(side="right", fill="y")
        widget_text.config(yscrollcommand=scrollbar_text.set)
        return widget_text

    def create_entry_message_input(self) -> tk.Entry:
        frame_message_input = tk.Frame(self.top_level_widget)
        frame_message_input.pack(pady=FRAMES_PADDING, fill="x")
        entry_message_input = tk.Entry(frame_message_input)
        entry_message_input.pack(side="left", fill="x", expand=True, padx=(0, FRAMES_PADDING))
        button_send = tk.Button(frame_message_input, text="Send", command=self.send_message)
        button_send.pack(side="right")
        return entry_message_input

    def create_button_game_state(self) -> None:
        frame_game_sate = tk.Frame(self.top_level_widget)
        frame_game_sate.pack(pady=FRAMES_PADDING, fill="x")
        button_find_new_match = tk.Button(frame_game_sate, text="Find New Match", command=self.find_new_match)
        button_find_new_match.pack(side="left")
        button_exit = tk.Button(frame_game_sate, text="Exit Game", command=self.stop)
        button_exit.pack(padx=12, side="left")

    def display_message(self, message) -> None:
        self.widget_message_display.config(state="normal")
        self.widget_message_display.insert("end", message + "\n")
        self.widget_message_display.see("end")
        self.widget_message_display.config(state="disabled")

    def send_message(self) -> str:
        message = self.entry_message_input.get().strip()
        if message:
            self.update_queue.put(("message", "[YOU]: " + message))
            if self.tcp_service:
                self.tcp_service.put_message(json.dumps({"type": "message", "data": message}))
                log_message(f"Chat message sent: {message}")
            self.entry_message_input.delete(0, tk.END)
        return message

    def find_new_match(self):
        # finds new match
        log_game("Finding new match requested")
        if self.tcp_service is None:
            self.update_queue.put(("start_tcp",))
        else:
            self.update_queue.put(("restart",))

    def consume_message_tcp(self):
        log_connection("TCP consumer thread started")
        while not self.shutdown.is_set():
            if self.tcp_service:
                msg = self.tcp_service.get_message()
                if msg is None:
                    break
                msg_json = json.loads(msg)
                log_message(f"TCP message received: {msg_json}")
                if msg_json["type"] == "control" and msg_json["data"]["flag"] == "init":
                    self.update_queue.put(("start_udp", msg_json))
                elif msg_json["type"] == "control" and msg_json["data"]["flag"] == "stats":
                    self.update_queue.put(("stats", msg_json))
                elif msg_json["type"] == "message":
                    self.update_queue.put(("message", "[OPPONENT]: " + msg_json["data"]))
        log_connection("TCP consumer thread stopped")

    def consume_message_udp(self):
        log_connection("UDP consumer thread started")
        while not self.shutdown.is_set():
            if self.udp_service:
                msg = self.udp_service.get_message()
                if msg is None:
                    break
                msg_json = json.loads(msg)
                log_message(f"UDP message received: {msg_json}")
                print(f"udp message {msg_json}")
                # match state
                res = msg_json["res"]
                if res == -3:
                    self.update_queue.put(("init_match", msg_json))
                elif res == -2:
                    self.update_queue.put(("update_match", msg_json))
                else:
                    self.update_queue.put(("end_match", msg_json))
        log_connection("UDP consumer thread stopped")

    def run(self):
        log_connection("Client main loop starting")
        self.top_level_widget.mainloop()

    def stop_services(self):
        log_connection("Stopping client services")
        self.shutdown.set()
        if self.tcp_service:
            self.tcp_service.stop()
            self.tcp_service = None
        if self.udp_service:
            self.udp_service.stop()
            self.udp_service = None
        if self.thread_tcp_consumer and self.thread_tcp_consumer.is_alive():
            self.thread_tcp_consumer.join()
            self.thread_tcp_consumer = None
        if self.thread_udp_consumer and self.thread_udp_consumer.is_alive():
            self.thread_udp_consumer.join()
            self.thread_udp_consumer = None

    def stop(self):
        self.stop_services()
        self.top_level_widget.quit()
        log_connection("Client stopped")

    def exit(self):
        self.update_queue.put(("exit",))


# GUI
CELL_SIZE = 100
BOARD_SIZE = 3
CANVAS_PADDING = 20
FRAMES_PADDING = 5
TEXT_WIDGET_HEIGHT = 12
# Networking
SOCKET_BACK_LOG = 128
MESSAGE_BUFFER_SIZE = 1024
SERVER_TCP_IP = "127.0.0.1"
SERVER_TCP_PORT = 55500
CLIENT_IP = "127.0.0.1"
CLIENT_UDP_PORT = 33333
CLIENT_TCP_PORT = 33334

if __name__ == '__main__':
    client = Client(SERVER_TCP_IP, SERVER_TCP_PORT, CLIENT_IP, CLIENT_TCP_PORT, CLIENT_UDP_PORT)
    client.run()
