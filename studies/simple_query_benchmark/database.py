import random


class SQBDatabase:
    def __init__(self, n_a, n_b):
        random.seed(0)  # Initialize the random generator with a seed to ensure reproducibility.
        self._a = [(i, random.randint(0, n_b - 1), random.randint(0, 9), random.randint(0, 99)) for i in range(n_a)]
        self._b = [(i, random.randint(0, 9), random.randint(0, 99)) for i in range(n_b)]
        random.shuffle(self._a)
        random.shuffle(self._b)

    @property
    def a(self):
        return self._a

    @property
    def b(self):
        return self._b
