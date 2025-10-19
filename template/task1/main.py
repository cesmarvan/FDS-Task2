import os
import time
import threading
import random

nodes = []
buffer = {}  # items are in the form 'node_id': [(msg_type, value)]


class Node:
    def __init__(self, id):
        buffer[id] = []
        self.id = id
        self.working = True
        self.state = 'follower'
        # i added:
        self.voted = None
        self.current_leader = None
        self.last_heartbeat = None
        self.candidate = False
        self.num_of_received_votes = set()
        self.election_timeout = None
        self.wait_until = None

    def start(self):
        print(f'node {self.id} started')
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        while True:
            while buffer[self.id]:
                msg_type, value = buffer[self.id].pop(0)
                if self.working:
                    self.deliver(msg_type, value)

            # If a follower doesn't get a heartbeat for 1 second then start election
            if self.working and self.state == 'follower':
                now = time.time()
                if self.last_heartbeat is None or (now - self.last_heartbeat > 1.0):
                    if not self.candidate and self.current_leader != self.id:
                        self.election()

            # When leader, send heartbeat
            if self.working and self.state == 'leader':
                self.broadcast("heartbeat", self.id)
                time.sleep(0.5)

            time.sleep(0.05)

    def broadcast(self, msg_type, value):
        if self.working:
            for node in nodes:
                if node.id != self.id:  # don't send the message to yourself
                    buffer[node.id].append((msg_type, value))

    def crash(self):
        if self.working:
            #Everything has to be None, False or empty so when recovering we don't have past values
            self.working = False
            buffer[self.id] = []
            self.state = 'crashed'
            self.current_leader = None
            self.last_heartbeat = None
            self.num_of_received_votes.clear()
            self.candidate = False

    def recover(self):
        if not self.working:
            buffer[self.id] = []
            self.working = True
            self.state = 'follower'
            self.last_heartbeat = None

    def deliver(self, msg_type, value):
        if not self.working:
            return

        if msg_type == "heartbeat":
            # Update leader and the moment when the HB is received
            if self.current_leader != value:
                self.current_leader = value
                if self.state == "follower":
                    print(f"node {self.id} got a heartbeat and followed node {value} as leader")
            self.last_heartbeat = time.time()
            

            if self.state != "follower":
                self.state = "follower"
                self.candidate = False
                self.voted = None

        elif msg_type == "candidacy":
            candidate_id = value
            # If while waiting for your candidacy to go through the waiting time you receive another candidacy message --> Abort your candidacy
            if self.state == 'candidate' and candidate_id != self.id:
                self.state = 'follower'
                self.wait_until = None
                self.candidate = False
                self.election_timeout = None
                print(f"node {self.id} voted to node {candidate_id}")
                self.voted = None

            # If the node hasn't voted yet, vote for the candidate. 
            if self.voted is None:
                self.voted = candidate_id
                print(f"node {self.id} voted to node {candidate_id}")
                self.broadcast("vote", (self.id, candidate_id))

        elif msg_type == "vote":
            voter_id, candidate_id = value
            if self.candidate and candidate_id == self.id:
                self.num_of_received_votes.add(voter_id)

    def election(self):
        # Change state to candidate
        self.state = 'candidate'
        self.current_leader = None
        self.voted = None
        self.num_of_received_votes.clear()
        self.candidate = True

        self.wait_until = time.time() + random.uniform(1.0, 3.0)
        print(f'node {self.id} is starting an election')

        # We wait randomly between 1 and 3 seconds while listening to possible messages being sent
        while self.wait_until and time.time() < self.wait_until:
            if buffer[self.id]:
                msg_type, value = buffer[self.id].pop(0)
                self.deliver(msg_type, value)
            time.sleep(0.05)

        # Broadcast the candidacy message and vote for yourself
        if self.working and self.state == 'candidate':
            self.broadcast('candidacy', self.id)
            self.voted = self.id
            self.num_of_received_votes = {self.id}
            self.election_timeout = time.time() + 2.0

            # Wait for other nodes to vote
            while self.election_timeout is not None and time.time() < self.election_timeout:
                if buffer[self.id]:
                    msg_type, value = buffer[self.id].pop(0)
                    self.deliver(msg_type, value)

                # If you have more than half the votes then the node gets to be the leader
                if len(self.num_of_received_votes) > len(nodes) // 2:
                    self.state = 'leader'
                    self.current_leader = self.id
                    self.last_heartbeat = time.time()
                    self.election_timeout = None 
                    print(f'node {self.id} detected node {self.current_leader} as leader')
                    return
                time.sleep(0.05)

            # When the candidate doesn't have enough votes then state goes back to follower
            self.state = 'follower'
            self.voted = None
            self.num_of_received_votes.clear()
            self.election_timeout = None
            self.last_heartbeat = None


def initialize(N):
    global nodes
    nodes = [Node(i) for i in range(N)]
    for node in nodes:
        node.start()


if __name__ == "__main__":
    os.system('clear')
    N = 3
    initialize(N)
    print('actions: state, crash, recover')
    while True:
        act = input('\t$ ')
        if act == 'crash':
            id = int(input('\tid > '))
            if 0 <= id < N:
                nodes[id].crash()
        elif act == 'recover':
            id = int(input('\tid > '))
            if 0 <= id < N:
                nodes[id].recover()
        elif act == 'state':
            for node in nodes:
                print(f'\t\tnode {node.id}: {node.state}')

