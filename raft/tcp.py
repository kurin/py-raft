import socket
import errno
import struct
import thread
import select

from raft.bijectivemap import create_map

class TCP(object):
    def __init__(self, port, uuid):
        self.port = port
        self.connections = {}
        self.c2u, self.u2c = create_map()
        self.data = {}
        self.unknowns = set()
        self.uuid = uuid

    def start(self):
        self.running = True
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.srv.bind(("", self.port))
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                # something is already there; just bind to anything for now
                self.srv.bind(("", 0))
                self.port = self.srv.getsockname()[1]
            else:
                raise e
        thread.start_new_thread(self.accept, ())

    def connect(self, addr):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conn.connect(addr)
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED:
                return None
        self.add_unknown(conn)
        return True

    def accept(self):
        self.srv.listen(5)
        while self.running:
            try:
                conn, addr = self.srv.accept()
                self.add_unknown(conn)
            except socket.error as e:
                if e.errno == errno.ECONNABORTED:
                    continue

    def read(self, timeout=0.25):
        recv, _, _ = select.select(self.c2u.keys(), [], [], timeout)
        rcvd = []
        for conn in recv:
            msgs = self.read_conn_msg(conn)
            if msgs is not None:
                uuid = self.c2u[conn]
                rcvd.append((uuid, msgs))
        self.read_unknowns()
        return rcvd

    def add_unknown(self, conn):
        self.unknowns.add(conn)
        msgsize = struct.pack("!I", len(self.uuid) + struct.calcsize("!I"))
        conn.send(msgsize + self.uuid)
        self.read_unknowns()

    def read_unknowns(self):
        recv, _, _ = select.select(list(self.unknowns), [], [], 0.01)
        for conn in recv:
            uuid = self.read_conn_msg(conn, 1)[0]
            if uuid is not None:
                self.u2c[uuid] = conn

    def read_conn_msg(self, conn, msgnum=0):
        data = conn.recv(4092)
        buff = self.data.get(conn, '')
        buff += data
        self.data[conn] = buff
        msgs = []
        for count, msg in enumerate(self.extract_msg(conn)):
            msgs.append(msg)
            if msgnum and count >= msgnum:
                return msgs
        return msgs

    def extract_msg(self, conn):
        buff = self.data[conn]
        isize = struct.calcsize("!I")
        if len(buff) < isize:
            # can't even get the length of the next message
            return
        while len(buff) > isize:
            size = struct.unpack("!I", buff[0:isize])[0]
            if len(buff) < size:
                return
            msg = buff[isize:size]
            buff = buff[size:]
            self.data[conn] = buff
            yield msg

    def send(self, msg, uuid):
        msgsize = struct.pack("!I", len(msg) + struct.calcsize("!I"))
        conn = self.u2c[uuid]
        try:
            conn.send(msgsize + msg)
        except socket.error as e:
            if e.errno == errno.EPIPE:
                addr = conn.getsockname()
                self.connect(addr)

    def shutdown(self):
        try:
            self.running = False
            self.srv.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:
            pass
