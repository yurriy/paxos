class Prepare(object):
    def __init__(self, acceptor_id, round_id):
        self.acceptor_id = acceptor_id
        self.round_id = round_id


class Propose(object):
    def __init__(self, round_id, value):
        self.round_id = round_id
        self.value = value


class Accept(object):
    def __init__(self, acceptor_id, round_id, proposed_round, value):
        self.acceptor_id = acceptor_id
        self.round_id = round_id
        self.proposed_round = proposed_round
        self.value = value


class Proposer(object):
    def __init__(self, process_count):
        # type: (int, int) -> None
        self.process_count = process_count
        self.current_round = -1
        self.current_value = None
        self.prepared = dict()

    def on_propose(self, round_id, value):
        # type: (int, str) -> iter[Prepare]
        self.current_round = round_id
        self.current_value = value
        self.prepared = dict()
        for acceptor_id in range(1, self.process_count):
            yield Prepare(acceptor_id, round_id)

    def on_prepared(self, acceptor_id, round_id, voted_round, voted_value):
        # type: (int, int, int, str) -> iter[Accept]
        if self.current_round != round_id:
            return
        self.prepared[acceptor_id] = (voted_round, voted_value)
        if len(self.prepared) >= self.process_count / 2:
            latest_round = -1
            for voted_round, voted_value in self.prepared.values():
                if latest_round < voted_round:
                    latest_round = voted_round
                    self.current_value = voted_value
            proposed_round = latest_round if latest_round != -1 else self.current_round
            for acceptor_id in self.prepared.keys():
                yield Accept(acceptor_id, self.current_round, proposed_round, self.current_value)
            self.prepared = dict()
