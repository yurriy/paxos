from collections import defaultdict


class Learner(object):
    def __init__(self, process_count):
        # type: (int, int) -> None
        self.process_count = process_count
        self.accepted = defaultdict(set)
        self.chosen_value = None
        self.round_id = None
        self.requests_queue = []

    def on_learn(self, acceptor_id, round_id, value):
        # type: (int, str) -> None
        self.accepted[round_id].add((acceptor_id, value))
        if len(self.accepted[round_id]) >= self.process_count / 2:
            self.chosen_value = value
            self.round_id = round_id
