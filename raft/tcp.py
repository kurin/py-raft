import socket
import errno
import struct
import thread
import select

from raft.bijectivemap import create_map

def start(port, uuid):
    tcp = TCP(port, uuid)
    tcp.start()
    return tcp

class TCP(object):
    def __init__(self, port, uuid):
        self.port = port
        self.connections = {}
        self.c2u, self.u2c = create_map()
        self.data = {}
        self.unknowns = set()
        self.a2c, self.c2a = create_map()
        self.uuid = uuid

    def __contains__(self, uuid):
        return uuid in self.u2c

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
        if addr in self.a2c:
            return
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.a2c[addr] = conn
        try:
            conn.connect(addr)
        except socket.error as e:
            del self.a2c[addr]
            if e.errno == errno.ECONNREFUSED:
                return None
        conn.setblocking(0)
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

    def recv(self, timeout=0):
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
        try:
            sent = 0
            msg = msgsize + self.uuid
            while sent < len(msg):
                sent += conn.send(msg[sent:], socket.MSG_DONTWAIT)
        except socket.error:
            return
        self.read_unknowns()

    def read_unknowns(self):
        recv, _, _ = select.select(list(self.unknowns), [], [], 0)
        for conn in recv:
            uuid = self.read_conn_msg(conn, 1)
            if uuid:
                uuid = uuid[0]
                self.u2c[uuid] = conn
                self.unknowns.remove(conn)
                try:
                    del self.c2a[conn]
                except KeyError:
                    pass  # we didn't initiate

    def read_conn_msg(self, conn, msgnum=0):
        try:
            data = conn.recv(4092)
        except socket.error:
            self.remconn(conn)
            return
        if data == '':
            self.remconn(conn)
            if conn in self.c2u:
                del self.c2u[conn]
            if conn in self.data:
                del self.data[conn]
            return
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
        try:
            conn = self.u2c[uuid]
        except KeyError:
            return
        try:
            sent = 0
            msg = msgsize + msg
            while sent < len(msg):
                sent += conn.send(msg[sent:], socket.MSG_DONTWAIT)
        except socket.error as e:
            if e.errno == errno.EPIPE:
                addr = conn.getsockname()
                self.connect(addr)

    def remconn(self, conn):
        if conn in self.c2u:
            del self.c2u[conn]
        if conn in self.data:
            del self.data[conn]

    def shutdown(self):
        try:
            self.running = False
            self.srv.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:
            pass
