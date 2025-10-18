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
        threading.Thread(target=self.run).start()

    def run(self):
        while True:
            while buffer[self.id]:
                msg_type, value = buffer[self.id].pop(0)
                if self.working:
                    self.deliver(msg_type, value)
            if self.working and self.state == 'follower':
                current_time = time.time()

                # Here we check if we have to start election
                if self.last_heartbeat is None or (current_time - self.last_heartbeat > 1.0):
                    if not self.candidate and self.current_leader != self.id:
                        self.election()

            time.sleep(0.1)

    def broadcast(self, msg_type, value):
        if self.working:
            for node in nodes:
                buffer[node.id].append((msg_type, value))

    def crash(self):
        if self.working:
            self.working = False
            buffer[self.id] = []
            self.state = 'crashed'

    def recover(self):
        if not self.working:
            buffer[self.id] = []
            self.working = True
            self.state = 'follower'
            self.last_heartbeat = 0.0

    def deliver(self, msg_type, value):
        # I have to double check this
        if not self.working:
            return
        if msg_type == "heartbeat":
            # Update leader info and heartbeat time
            self.current_leader = value
            self.last_heartbeat = time.time()

            if self.state != "follower":
                print(f"Node {self.id} now follows leader {value}")
                self.state = "follower"
                self.candidate = False
                self.voted = None

        elif msg_type == "candidacy":
            candidate_id = value
            # if node receives another candidacy message before the waiting time ends then cancel candidacy
            if self.state == 'candidate' and time.time() < self.wait_until and candidate_id != self.id:
                print(f'candidacy of node {self.id} aborted')
                self.state = 'follower'
                self.wait_until = None
                self.candidate = False
                self.election_timeout = None
                self.voted = None

            if self.voted is None:
                self.voted = candidate_id
                print(f"Node {self.id} votes for node {candidate_id}")
                self.broadcast("vote", (self.id, candidate_id))

        elif msg_type == "vote":
            voter_id, candidate_id = value
            if self.candidate and candidate_id == self.id:
                self.num_of_received_votes.add(voter_id)

    def election(self):
        # Update state to candidate and clear all variables related to election
        self.state = 'candidate'
        self.current_leader = None
        self.voted = None
        self.num_of_received_votes.clear()
        self.candidate = True

        # wait between 1 and 3 seconds for other candidacy messages
        self.wait_until = time.time() + random.uniform(1.0, 3.0)
        print(f'node {self.id} started an election')

        start_wait = time.time()
        while self.wait_until and time.time() < self.wait_until:
            # we make it possible to recieve messages while waiting
            if buffer[self.id]:
                msg_type, value = buffer[self.id].pop(0)
                self.deliver(msg_type, value)
            time.sleep(0.05)

        if self.working and time.time() and self.wait_until is not None:
            if time.time() >= self.wait_until:
                # send candidacy message and vote for yourself
                self.broadcast('candidacy', self.id)
                print(f'node {self.id} sent a candidacy message')
                self.voted = self.id
                self.num_of_received_votes = {self.id}
                self.election_timeout = time.time() + 2.0
                while time.time() < self.election_timeout:
                    time.sleep(0.05)

            if self.election_timeout is not None and len(self.num_of_received_votes) > len(nodes) / 2:
                if time.time() >= self.election_timeout:
                    print(f'node {self.id} is now the leader')
                    self.state = 'leader'
                    self.election_timeout = None
                    self.current_leader = self.id
                    self.last_heartbeat = 0.0
            # if you don't win the election then clear election variables and change state back to follower
            else:
                self.state = 'follower'
                self.voted = None
                self.num_of_received_votes.clear()
                self.election_timeout = None
                self.last_heartbeaet = None


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
            if 0 <= id and id < N:
                nodes[id].crash()
        elif act == 'recover':
            id = int(input('\tid > '))
            if 0 <= id and id < N:
                nodes[id].recover()
        elif act == 'state':
            for node in nodes:
                print(f'\t\tnode {node.id}: {node.state}')

