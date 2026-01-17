from sim import Simulator
import pandas as pd


def experiments(simulator : Simulator, stress_test=False, spike_server_enhanced=False):
    for si_max in range(10, 170, 10):
        simulator.SI_max = si_max
        if stress_test:
            for arrival_rate in range(1, 13):  # Da 1 req/s a 12 req/s
                simulator.arrival_mean = 1.0 / arrival_rate
                if spike_server_enhanced:
                    simulator.spike_mean = 0.5 * simulator.spike_mean  # Potenziamento del doppio
                params, stats = simulator.run()
                simulator.reset()
        else:
            params, stats = simulator.run()
            simulator.reset()
    return params, stats

if __name__ == "__main__":
    sim = Simulator()
    # Esperimento con SI_max variabile tra 10 e 160 per capire il migliore
    params, stats = experiments(simulator=sim)

    # Stress test con carico crescente variando sia SI_max tra 10 e 160 che l'arrival rate da 1 req/s a 12 req/s
    params, stats = experiments(simulator=sim, stress_test=True)
    # Stesso espermento dello stress test ma con spike server potenziato del doppio
    params, stats = experiments(simulator=sim, stress_test=True, spike_server_enhanced=True)