from collections import defaultdict
from public import Process, ClientProtocol, Context
from paxos import Proposer, Acceptor, Learner
from paxos.proposer import Propose, Prepare, Accept
from paxos.acceptor import Prepared, Learn

CP = ClientProtocol


def serialize(msg, key):
    result = msg.__dict__
    result[CP.METHOD] = 'internal'
    result[CP.KEY] = key
    result['cls'] = type(msg).__name__
    return result


def deserialize(msg):
    for cls in [Prepare, Accept, Prepared, Learn]:
        if cls.__name__ == msg['cls']:
            result = cls.__new__(cls)
            result.__dict__ = msg
            return result
    raise ValueError('Message class %s not found' % msg['cls'])


class PaxosProcess(Process):
    def __init__(self, pid):
        super(PaxosProcess, self).__init__(pid)
        self.process_count = 0
        self.proposer = defaultdict(lambda: Proposer(self.process_count))
        self.acceptor = defaultdict(lambda: Acceptor(self.process_count))
        self.learner = defaultdict(lambda: Learner(self.process_count))
        self.client_requests = []
        self.internal_requests = []

    def on_setup(self, process_count):
        self.process_count = process_count

    def on_tick(self, ctx):
        # type: (Context) -> None
        internal_requests = self.internal_requests
        self.internal_requests = []
        for sender, key, msg in internal_requests:
            self.process_internal_request(ctx, sender, key, msg)
        client_requests = self.client_requests
        self.client_requests = []
        for sender, msg in client_requests:
            self.process_client_request(ctx, sender, msg)

    def send(self, ctx, recipient, msg):
        # type: (Context, int, object) -> None
        if recipient == self.pid:
            self.on_receive(ctx, self.pid, msg)
        else:
            ctx.send(recipient, msg)

    def process_client_request(self, ctx, sender, msg):
        reqid = msg[CP.ID]
        learner = self.learner[msg[CP.KEY]]
        if learner.chosen_value is not None:
            answer = {CP.ID: reqid, CP.VALUE: learner.chosen_value}
            if msg[CP.METHOD] == 'set':
                answer[CP.FLAG] = learner.proposed_round == reqid
            ctx.send(sender, answer)
        else:
            self.client_requests.append((sender, msg))

    def process_internal_request(self, ctx, sender, key, msg):
        # type: (Context, int, object) -> None
        proposer, acceptor, learner = self.proposer[key], self.acceptor[key], self.learner[key]
        if isinstance(msg, Propose):
            for prepare in proposer.on_propose(msg.round_id, msg.value):
                self.send(ctx, prepare.acceptor_id, serialize(prepare, key))
        elif isinstance(msg, Prepare):
            prepared = acceptor.on_prepare(sender, msg.round_id)
            if prepared is not None:
                self.send(ctx, sender, serialize(prepared, key))
        elif isinstance(msg, Accept):
            for learn in acceptor.on_accept(msg.round_id, msg.proposed_round, msg.value):
                self.send(ctx, learn.learner_id, serialize(learn, key))
        elif isinstance(msg, Prepared):
            for accept in proposer.on_prepared(sender, msg.round_id, msg.voted_round, msg.voted_value):
                self.send(ctx, accept.acceptor_id, serialize(accept, key))
        elif isinstance(msg, Learn):
            learner.on_learn(sender, msg.round_id, msg.proposed_round, msg.value)
        else:
            raise NotImplementedError('Message class %s is unknown' % type(msg))

    def on_receive(self, ctx, sender, msg):
        # type: (Context, int, object) -> None
        if isinstance(msg, dict):
            if msg[CP.METHOD] == 'get':
                self.client_requests.append((sender, msg))
            elif msg[CP.METHOD] == 'set':
                self.client_requests.append((sender, msg))
                self.internal_requests.append((sender, msg[CP.KEY], Propose(msg[CP.ID], msg[CP.VALUE])))
            elif msg[CP.METHOD] == 'internal':
                self.internal_requests.append((sender, msg[CP.KEY], deserialize(msg)))
        else:
            raise TypeError('Unexpected message type %s' % type(msg))
