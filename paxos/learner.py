from collections import defaultdict


class Learner(object):
    def __init__(self, process_count):
        # type: (int, int) -> None
        self.process_count = process_count
        self.accepted = defaultdict(set)
        self.chosen_value = None
        self.proposed_round = None
        self.requests_queue = []

    def on_learn(self, acceptor_id, round_id, proposed_round, value):
        # type: (int, str) -> None
        self.accepted[round_id].add((acceptor_id, value))
        if len(self.accepted[round_id]) >= self.process_count / 2:
            self.proposed_round = proposed_round
            self.chosen_value = value
