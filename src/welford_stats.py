import math

class WelfordStats:
    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0  # Somma dei quadrati delle differenze

    def update(self, x):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self):
        if self.n < 2:
            return 0.0
        return self.M2 / (self.n - 1)

    @property
    def std_dev(self):
        return math.sqrt(self.variance)

    def confidence_interval_95(self):
        # Per N > 30, usiamo z = 1.96 per il 95%
        # Se N è piccolo servirebbe la t-student, ma con 100 repliche 1.96 è ok.
        if self.n < 2:
            return 0.0
        se = self.std_dev / math.sqrt(self.n) # Errore Standard
        return 1.96 * se