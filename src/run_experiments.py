from sim import Simulator
import pandas as pd
import time
import logging

# Configurazione logging per debug
logging.basicConfig(level=logging.INFO, format='%(asctime)s[%(levelname)s] - %(message)s', datefmt='%H:%M:%S')

def compose_row(params, stats):
    SI_max, arrival_mean, web_mean, spike_mean, cv = params
    row = {
        "SI_max": SI_max,
        "Arrival_Mean": arrival_mean,
        "Web_Mean": web_mean,
        "Spike_Mean": spike_mean,
        "CV": cv,
        "Arrival_Rate": round(1.0 / arrival_mean, 0)
    }
    stats.pop("transient_response_times", None)  # Rimuovo i transient response times se presenti
    for metric, w in stats.items():
        row[f"{metric}_Mean"] = w.mean
        row[f"{metric}_Variance"] = w.variance
        row[f"{metric}_CI95"] = w.confidence_interval_95()
    return row

def experiments(simulator : Simulator, stress_test=False, spike_server_enhanced=False):
    buffer = []
    for si_max in range(10, 170, 10):  # Da 10 a 160 con step di 10
        simulator.SI_max = si_max
        if stress_test:
            for arrival_rate in range(1, 13):  # Da 1 req/s a 12 req/s
                simulator.arrival_mean = 1.0 / arrival_rate
                if spike_server_enhanced:
                    simulator.spike_mean = 0.5 * simulator.spike_mean  # Potenziamento del doppio
                logging.info(f"Eseguendo esperimento di stress test con SI_max = {si_max} e arrival rate = {arrival_rate} req/s e spike server potenziato: {spike_server_enhanced}")
                params, stats = simulator.run()
                row = compose_row(params, stats)
                buffer.append(row)
                simulator.reset()
        else:
            logging.info(f"Eseguendo esperimento base con SI_max = {si_max}")
            params, stats = simulator.run()
            row = compose_row(params, stats)
            buffer.append(row)
            simulator.reset()
    df = pd.DataFrame(buffer)
    return df

def transient(simulator : Simulator, SI_max_list, arrival_rate_list):
    buffer = []
    simulator.BIAS_PHASE = 0.0 
    for si_max in SI_max_list:
        simulator.SI_max = si_max
        for arrival_rate in arrival_rate_list:
            simulator.arrival_mean = 1.0 / arrival_rate
            logging.info(f"Eseguendo esperimento transitorio con SI_max = {si_max} e arrival rate = {arrival_rate} req/s")
            params, stats = simulator.run()
            transient_times = stats.pop("transient_response_times", None)
            if transient_times is not None:
                for idx, w in enumerate(transient_times):
                    t = idx * Simulator.SAMPLING_INTERVAL
                    row = {
                        "SI_max": si_max,
                        "Arrival_Rate": arrival_rate,
                        "Time": t,
                        "Transient_Response_Time_Mean": w.mean,
                        "Transient_Response_Time_Variance": w.variance,
                        "Transient_Response_Time_CI95": w.confidence_interval_95()
                    }
                    buffer.append(row)
            simulator.reset()
    df = pd.DataFrame(buffer)
    return df

if __name__ == "__main__":
    sim = Simulator()

    # start_time = time.time()
    # # Esperimento con SI_max variabile tra 10 e 160 per capire il migliore
    # df1 = experiments(simulator=sim)
    # end_time = time.time()
    # logging.info("Esperimenti completati in %.2f secondi.", end_time - start_time)
    # df1.to_csv("src/data/experiment_si_max.csv", index=False)

    # start_time = time.time()
    # # Stress test con carico crescente variando sia SI_max tra 10 e 160 che l'arrival rate da 1 req/s a 12 req/s
    # df2 = experiments(simulator=sim, stress_test=True)
    # end_time = time.time()
    # logging.info("Esperimenti di stress test completati in %.2f secondi.", end_time - start_time)
    # df2.to_csv("src/data/experiment_stress_test.csv", index=False)

    # start_time = time.time()
    # # Stesso espermento dello stress test ma con spike server potenziato del doppio
    # df3 = experiments(simulator=sim, stress_test=True, spike_server_enhanced=True)
    # end_time = time.time()
    # logging.info("Esperimenti di stress test con spike server potenziato completati in %.2f secondi.", end_time - start_time)
    # df3.to_csv("src/data/experiment_stress_test_enhanced_spike.csv", index=False)

    # Esperimento con SI_max fisso a 100 e arrival rate fisso a 6 req/s, variando il coefficiente di variazione CV
    # df = experiments(simulator=sim, stress_test=True)
    # df.to_csv("src/data/experiment_inf_si_max.csv", index=False)

    start_time = time.time()
    # Esperimento transitorio per osservare l'andamento del tempo di risposta nel tempo
    df4 = transient(simulator=sim, SI_max_list=range(10, 170, 10), arrival_rate_list=range(1, 13))
    end_time = time.time()
    logging.info("Esperimenti transitori completati in %.2f secondi.", end_time - start_time)
    df4.to_csv("src/data/experiment_transient_response_time.csv", index=False)

