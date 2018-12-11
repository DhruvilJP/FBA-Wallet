from __future__ import print_function

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
import pickledb


class MulticastPingPong(DatagramProtocol):

    def __init__(self, db, port):
        self.db = db
        self.port = port
        self.peers = [3001, 3002, 3003]
        self.voting_history = list()
        self.current_val = None
        self.q = Quorum(self.port)
        self.vote = None

    def startProtocol(self):
        self.transport.setTTL(5)
        self.transport.joinGroup("228.0.0.5")

    def datagramReceived(self, datagram, address):
        # print("Datagram %s received from %s" % (repr(datagram), repr(address)))
        if "foo" in datagram.decode('utf-8') or "bar" in datagram.decode('utf-8'):
            if self.port == "3000":
                for peer in self.peers:
                    self.transport.write(datagram, ("228.0.0.5", peer))
            message = datagram.decode('utf-8')
            key = message.split(':')[0]
            value = message.split('$')[1]
            if (db.get(key) != False):
                cur_val = db.get(key)
                db.set(key, int(cur_val) + int(value))
                new_val = db.get(key)
                data = str(key) + ":" + "$" + str(new_val)
            else:
                db.set(key, value)
                data = key + ":" + "$" + value
            print(data)
            # if self.port == "3000":
            #     self.transport.write(data.encode('utf-8'), address)
        elif "end" in datagram.decode('utf-8'):
            if self.port == "3000":
                for peer in self.peers:
                    self.transport.write(datagram, ("228.0.0.5", peer))
            
            # voting open for the value stored in db
            self.vote = Voting(self.port, "open")
            assert_message = self.vote.open(str(db.get('foo')))
            self.current_val = str(db.get('foo'))
            self.voting_history.append(assert_message)

            # sending open to all peers of the quorum slice
            for peer in self.q.quorum:
                if peer != self.port:
                    self.transport.write(assert_message.encode('utf-8'), ("228.0.0.5", int(peer)))

        elif "accept" in datagram.decode('utf-8'):
            # # ratifying the agreed value
            if ("Vote --> " + self.port + " : " + "ratify" + " : " + self.current_val) not in self.voting_history:
                ratify = True
                for vpeer in self.q.v_blocking:
                    if (("Vote --> " + vpeer + " : " + "ratify" + " : " + self.current_val) in self.voting_history) or (("Vote --> " + vpeer + " : " + "accept" + " : " + self.current_val) in self.voting_history):
                        continue
                    else:
                        print("ratify is false")
                        accept = False #Cannot accept the value just yet

                if ratify == False:
                    for qpeer in self.q.quorum:
                        if (("Vote --> " + qpeer + " : " + "open" + " : " + self.current_val) in self.voting_history) or (("Vote --> " + qpeer + " : " + "accept" + " : " + self.current_val) in self.voting_history):
                            continue
                        else:
                            print("ratify is false")
                            accept = False
                    
                if ratify == True:
                    ratify_message = self.vote.ratify(self.current_val)
                    self.voting_history.append(ratify_message)
                    for peer in self.q.quorum:
                        if peer != self.port:
                             self.transport.write(ratify_message.encode('utf-8'), ("228.0.0.5", int(peer)))
            
            else:
                ratify_message = self.vote.ratify(self.current_val)
                self.voting_history.append(ratify_message)
                for peer in self.q.quorum:
                    if peer != self.port:
                         self.transport.write(ratify_message.encode('utf-8'), ("228.0.0.5", int(peer)))
            

        elif "open" in datagram.decode('utf-8'):
            self.voting_history.append(datagram.decode('utf-8'))

            # accepting a value with agreement
            if ("Vote --> " + self.port + " : " + "accept" + " : " + self.current_val) not in self.voting_history:
                accept = True
                for vpeer in self.q.v_blocking:
                    if (("Vote --> " + vpeer + " : " + "open" + " : " + self.current_val) in self.voting_history) or (("Vote --> " + vpeer + " : " + "accept" + " : " + self.current_val) in self.voting_history):
                        continue
                    else:
                        print("accept is false")
                        accept = False #Cannot accept the value just yet

                if accept == False:
                    for qpeer in self.q.quorum:
                        if (("Vote --> " + qpeer + " : " + "open" + " : " + self.current_val) in self.voting_history) or (("Vote --> " + qpeer + " : " + "accept" + " : " + self.current_val) in self.voting_history):
                            continue
                        else:
                            accept = False

                if accept == True:
                    accept_message = self.vote.accept(self.current_val)
                    self.voting_history.append(accept_message)
                    for peer in self.q.quorum:
                        if peer != self.port:
                            self.transport.write(accept_message.encode('utf-8'), ("228.0.0.5", int(peer)))
            
            else:
                accept_message = self.vote.accept(self.current_val)
                self.voting_history.append(accept_message)
                for peer in self.q.quorum:
                    if peer != self.port:
                        self.transport.write(accept_message.encode('utf-8'), ("228.0.0.5", int(peer)))
                        
            

            # #confirm the ratified value
            for peer in self.q.quorum:
                confirm = True
                if ("Vote --> " + peer + " : " + "ratify" + " : " + self.current_val) in self.voting_history:
                    continue
                else:
                    confirm = False #Cannot confirm the value just yet

            if confirm == True:
                confirmation = self.vote.confirm(self.current_val)
                self.voting_history.append(confirm)
                for peer in self.q.quorum:
                    if peer != self.port:
                        self.transport.write(confirmation.encode('utf-8'), ("228.0.0.5", int(peer)))
                
                self.db.set('foo', self.current_val)
        


class Quorum():

    def __init__(self, port):
        self.quorum = ["3000", "3001", "3002", "3003"]
        self.quorum_slices = [["3000", "3001", "3002"],["3001", "3002", "3003", "3000"], ["3002", "3003", "3000", "3001"], ["3003", "3000", "3001", "3002"]]
        self.v_blocking = []
        for peer in self.quorum:
            if peer != port:
                self.v_blocking.append(peer)     

class Voting():

    def __init__(self, node, state):
        self.node = node
        self.state = state
        self.voted = dict()
        self.voting = list()

    def open(self, value):
        print("Open for any option")
        self.voting.append("Vote --> {} : {} : {}".format(self.node, self.state, value))
        return self.voting[-1]
        
    def accept(self, acc_val):
        print("accepted -> " + acc_val)
        self.voting.append("Vote --> {} : {} : {}".format(self.node, "accept", acc_val))
        self.state = "accept"
        return self.voting[-1]

    def ratify(self, acc_val):
        print("ratified -> " + acc_val)
        self.voting.append("Vote --> {} : {} : {}".format(self.node, "ratify", acc_val))
        self.state = "ratify"
        return self.voting[-1]

    def confirm(self, acc_val):
        print("confirmed -> " + acc_val)
        self.voting.append("Vote --> {} : {} : {}".format(self.node, "confirmed", acc_val))
        self.state = "confirmed"
        return self.voting[-1]

        

if __name__ == "__main__":
    port = sys.argv[1]
    dbname = "assignment3_" + str(port) + ".db"
    db = pickledb.load(dbname, False)
    reactor.listenMulticast(int(port), MulticastPingPong(db, port),
                        listenMultiple=True)
    reactor.run()
    # db.dump()