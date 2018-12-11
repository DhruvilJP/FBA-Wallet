from __future__ import print_function

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys


class MulticastPingClient(DatagramProtocol):

    def __init__(self, port):
        self.port = port
        self.messages = ["foo:$10", "bar:$30", "foo:$20", "bar:$20", "foo:$30", "bar:$10", "end"]

    def startProtocol(self):
        self.transport.joinGroup("228.0.0.5")
        for message in self.messages:
            self.transport.write(message.encode('utf-8'), ("228.0.0.5", int(self.port)))

    def datagramReceived(self, datagram, address):
        print("%s <---- node id: %s" % (repr(datagram), repr(address)))

if __name__ == "__main__":
    port = sys.argv[1]
    reactor.listenMulticast(9999, MulticastPingClient(port), listenMultiple=True)
    reactor.run()