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
    if msg[CP.METHOD] == 'set':
        return Propose(msg[CP.ID], msg[CP.VALUE])
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
        self.proposers = defaultdict(lambda: Proposer(self.process_count))
        self.acceptors = defaultdict(lambda: Acceptor(self.process_count))
        self.learners = defaultdict(lambda: Learner(self.process_count))
        self.client_requests = []
        self.internal_requests = []

    def on_setup(self, process_count):
        self.process_count = process_count

    def on_tick(self, ctx):
        # type: (Context) -> None

        requests = self.internal_requests
        self.internal_requests = []
        for sender, key, msg in requests:
            self.process(ctx, sender, key, msg)

        requests = self.client_requests
        self.client_requests = []
        for sender, msg in requests:
            reqid = msg[CP.ID]
            learner = self.learners[msg[CP.KEY]]
            value = learner.chosen_value
            if value is not None:
                answer = {CP.ID: reqid, CP.VALUE: value}
                if msg[CP.METHOD] == 'set':
                    answer[CP.FLAG] = learner.round_id == reqid
                ctx.send(sender, answer)
            else:
                self.client_requests.append((sender, msg))

    def send(self, ctx, recipient, msg):
        # type: (Context, int, object) -> None
        if recipient == self.pid:
            self.on_receive(ctx, self.pid, msg)
        else:
            ctx.send(recipient, msg)

    def process(self, ctx, sender, key, msg):
        # type: (Context, int, object) -> None
        if isinstance(msg, Propose):
            for prepare in self.proposers[key].on_propose(msg.round_id, msg.value):
                self.send(ctx, prepare.acceptor_id, serialize(prepare, key))
        elif isinstance(msg, Prepare):
            prepared = self.acceptors[key].on_prepare(sender, msg.round_id)
            if prepared is not None:
                self.send(ctx, sender, serialize(prepared, key))
        elif isinstance(msg, Accept):
            for learn in self.acceptors[key].on_accept(msg.round_id, msg.value):
                self.send(ctx, learn.learner_id, serialize(learn, key))
        elif isinstance(msg, Prepared):
            for accept in self.proposers[key].on_prepared(sender, msg.round_id, msg.voted_round, msg.voted_value):
                self.send(ctx, accept.acceptor_id, serialize(accept, key))
        elif isinstance(msg, Learn):
            self.learners[key].on_learn(sender, msg.round_id, msg.value)
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
