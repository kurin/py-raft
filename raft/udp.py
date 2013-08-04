import socket
import errno
import hashlib
import select


class UDP(object):
    def __init__(self, port):
        self.port = port

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", self.port))

    def recv(self):
        recv, _, _ = select.select([self.sock], [], [], 0.250)
        if recv:
            try:
                return self.sock.recvfrom(65535)  # max udp size
            except socket.error as e:
                pass  # just drop it on the floor

    def send(self, msg, dst):
        try:
            self.sock.sendto(msg, dst)
        except socket.error as e:
            if e[0] == errno.EPIPE:
                self.shutdown()
                self.start()

    def shutdown(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:  # nobody cares
            pass
