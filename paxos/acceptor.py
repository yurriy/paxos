from typing import Optional


class Prepared(object):
    def __init__(self, proposer_id, round_id, voted_round, value):
        self.proposer_id = proposer_id
        self.round_id = round_id
        self.voted_round = voted_round
        self.voted_value = value


class Learn(object):
    def __init__(self, learner_id, round_id, proposed_round, value):
        self.learner_id = learner_id
        self.round_id = round_id
        self.proposed_round = proposed_round
        self.value = value


class Acceptor(object):
    def __init__(self, process_count):
        # type: (int, int) -> None
        self.process_count = process_count
        self.promised_round = -1
        self.voted_round = -1
        self.voted_value = -1

    def on_prepare(self, proposer_id, round_id):
        # type: (int, str) -> Optional[Prepared]
        if round_id >= self.promised_round:
            self.promised_round = round_id
            return Prepared(proposer_id, round_id, self.voted_round, self.voted_value)

    def on_accept(self, round_id, proposed_round, value):
        # type: (int, str) -> iter[Learn]
        if round_id < self.promised_round:
            return
        self.voted_round = round_id
        self.voted_value = value
        for learner_id in range(1, self.process_count):
            yield Learn(learner_id, round_id, proposed_round, value)
