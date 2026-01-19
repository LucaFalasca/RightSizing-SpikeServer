from rngs import MODULUS, STREAMS, plantSeeds, selectStream
from hyperexp import Hyperexponential
import logging
import numpy as np
import multiprocessing

from welford_stats import WelfordStats

# Configurazione logging per debug
logging.basicConfig(level=logging.INFO, format='%(asctime)s[%(levelname)s] - %(message)s', datefmt='%H:%M:%S')

# --- Classi per Statistiche e Tempi ---
class Track:
    def __init__(self):
        self.area_node_web = 0.0
        self.area_node_spike = 0.0

        self.area_busy_web = 0.0
        self.area_busy_spike = 0.0

        self.completed_web = 0
        self.completed_spike = 0

        self.scaling_actions = 0



# --- Programma Principale ---
class Simulator:
    START      = 0.0
    BIAS_PHASE = 120.0         # Fase Transitoria di 120 secondi
    STOP       = 1200.0         # Simuliamo fino a 1200 secondi
    INFINITY   = 1e15
    SEED = 8
    REPLICAS = 100
    N_PROCESSES = 12    # Numero di processi paralleli per eseguire le repliche, deve essere <= 85 se no non bastano gli stream RNG

    def __init__(self):
        # --- Parametri del Modello di default ---
        self._SI_max = Simulator.INFINITY                 # Soglia SI_max (da ottimizzare)
        self._arrival_mean = 0.15          # 400 req/min
        self._web_mean     = 0.16          
        self._spike_mean   = 0.16          # Tasso identico al web server
        self._cv           = 4.0           # Coefficiente di variazione richiesto 
        self._stream_usage = {}
        self.reset()

    @property
    def SI_max(self):
        return self._SI_max
    
    @SI_max.setter
    def SI_max(self, value):
        self._SI_max = value

    @property
    def arrival_mean(self):
        return self._arrival_mean
    
    @arrival_mean.setter
    def arrival_mean(self, value):
        self._arrival_mean = value

    @property
    def web_mean(self):
        return self._web_mean   
    
    @web_mean.setter
    def web_mean(self, value):
        self._web_mean = value

    @property
    def spike_mean(self):
        return self._spike_mean 
    
    @spike_mean.setter
    def spike_mean(self, value):
        self._spike_mean = value

    @property
    def cv(self):
        return self._cv 
    
    @cv.setter
    def cv(self, value):
        self._cv = value

    def set_parameters(self, SI_max, arrival_mean, web_mean, spike_mean, cv):
        self.SI_max = SI_max
        self.arrival_mean = arrival_mean
        self.web_mean = web_mean
        self.spike_mean = spike_mean
        self.cv = cv

    def get_parameters(self):
        return (self._SI_max, self._arrival_mean, self._web_mean, self._spike_mean, self._cv)
        
    def reset(self):
        plantSeeds(Simulator.SEED)
        self._stream_usage = {}

    def reset_seed(self):
        plantSeeds(Simulator.SEED)

    def _GetArrival(self, stream):
        selectStream(stream) # Stream 0 per gli arrivi
        self._track_rng_usage(stream, 2)  # l'iperesponenziale usa 2 RNG per ogni chiamata
        return Hyperexponential(self.arrival_mean, self.cv)

    def _GetServiceWeb(self, stream):
        selectStream(stream) # Stream 1 per il Web Server
        self._track_rng_usage(stream, 2)  # l'iperesponenziale usa 2 RNG per ogni chiamata
        return Hyperexponential(self.web_mean, self.cv)
    
    def _GetServiceSpike(self, stream):
        selectStream(stream) # Stream 2 per lo Spike Server 
        self._track_rng_usage(stream, 2)  # l'iperesponenziale usa 2 RNG per ogni chiamata
        return Hyperexponential(self.spike_mean, self.cv)
    
    def _track_rng_usage(self, stream, count=1):
        if stream not in self._stream_usage:
            self._stream_usage[stream] = 0
        self._stream_usage[stream] += count
    class Job:
        def __init__(self, arrival_time, service_demand, is_spike=False):
            self.arrival_time = arrival_time
            self.service_demand = service_demand
            self.remaining_work = service_demand
            self.is_spike = is_spike

    def run(self):
        queue_input = [multiprocessing.Queue() for _ in range(Simulator.N_PROCESSES)]
        queue_output = multiprocessing.Queue()

        for replica in range(Simulator.REPLICAS):
            target_queue = queue_input[replica % Simulator.N_PROCESSES]
            target_queue.put(replica)
            logging.debug(f"Replica #{replica} inviata al queue input")

        for worker in range(Simulator.N_PROCESSES):
            queue_input[worker].put(None)  # Segnali di terminazione
        
        processes = []
        for worker_id in range(Simulator.N_PROCESSES):
            p = multiprocessing.Process(target=self._run, args=(worker_id, queue_input[worker_id], queue_output, self._SI_max))
            processes.append(p)
            p.start()


        stats = {}

        # Processo i risultati intanto
        for i in range(Simulator.REPLICAS):
            result = queue_output.get()
            
            for key, value in result.items():
                if value is not None:
                    if key not in stats:
                        stats[key] = WelfordStats()
                    stats[key].update(value)
                
            if (i + 1) % 10 == 0:
                logging.info(f"Raccolte {i + 1}/{Simulator.REPLICAS} repliche...")

        for p in processes:
            p.join()

        return self.get_parameters(), stats

    def _run(self, worker_id, queue_input, queue_output, SI_max):
        plantSeeds(Simulator.SEED)
        arrival_stream = worker_id
        web_stream = Simulator.N_PROCESSES + worker_id
        spike_stream = 2 * Simulator.N_PROCESSES + worker_id
        

        input_replica = queue_input.get()
        logging.debug(f"Queue Input Replica: {input_replica}")
        while input_replica is not None:

            t = Simulator.START
            area = Track()
            t = Simulator.START
            time_to_next_arrival = self._GetArrival(arrival_stream)
            web_jobs = []
            spike_jobs = []

        
            while (t < Simulator.STOP):
                # Per trovare il prossimo evento devo vedere chi è che il tempo di completamento più piccolo e confrontarlo con il prossimo arrivo
                logging.debug(f"Current Time: {t}")

                logging.debug(f"Next Arrival Event at: {time_to_next_arrival}")
                min_work_web_job = None
                min_work_spike_job = None

                if len(web_jobs) > 0:
                    # trovo il job con il minimo lavoro rimanente nel web server
                    min_work_web_job = min(web_jobs, key=lambda job: job.remaining_work)
                    time_to_complete_web = min_work_web_job.remaining_work * len(web_jobs)
                    logging.debug(f"Time to Complete Web Job: {time_to_complete_web} (Remaining Work: {min_work_web_job.remaining_work})")
                if len(spike_jobs) > 0:
                    # trovo il job con il minimo lavoro rimanente nello spike server
                    min_work_spike_job = min(spike_jobs, key=lambda job: job.remaining_work)
                    time_to_complete_spike = min_work_spike_job.remaining_work * len(spike_jobs)
                    logging.debug(f"Time to Complete Spike Job: {time_to_complete_spike} (Remaining Work: {min_work_spike_job.remaining_work})")

                time_to_next_event = min(
                    time_to_next_arrival,
                    time_to_complete_web if len(web_jobs) > 0 else Simulator.INFINITY,
                    time_to_complete_spike if len(spike_jobs) > 0 else Simulator.INFINITY
                )
                logging.debug(f"Next Time Event at: {time_to_next_event}")

                # Aggiorno le aree sotto le curve per calcolare le statistiche di utilizzo e numero di job
                if t >= Simulator.BIAS_PHASE:
                    area.area_node_web += len(web_jobs) * time_to_next_event
                    area.area_node_spike += len(spike_jobs) * time_to_next_event

                    if len(web_jobs) > 0:
                        area.area_busy_web += time_to_next_event
                    if len(spike_jobs) > 0:
                        area.area_busy_spike += time_to_next_event

                # A questo punto aggiorno il tempo rimanente di lavoro per ogni job in servizio in base al tempo trascorso
                for job in web_jobs:
                    job.remaining_work -= time_to_next_event / len(web_jobs)  # Condivisione della CPU tra i job
                for job in spike_jobs:
                    job.remaining_work -= time_to_next_event / len(spike_jobs)  # Condivisione della CPU tra i job

                # A questo punto processo l'evento
                if min_work_web_job is not None and time_to_complete_web == time_to_next_event:
                    # Completamento di un job, lo leviamo dalla lista 
                    web_jobs.remove(min_work_web_job)
                    logging.debug(f"Completed Web Job Arrival: {min_work_web_job.arrival_time}")

                    # Aggiorno il tempo al prossimo arrivo
                    time_to_next_arrival -= time_to_next_event
                    
                    # Aggiorno le statistiche
                    if t > Simulator.BIAS_PHASE: 
                        area.completed_web += 1
                        #TODO
                elif min_work_spike_job is not None and time_to_complete_spike == time_to_next_event:
                    # Completamento di un job, lo leviamo dalla lista 
                    spike_jobs.remove(min_work_spike_job)
                    logging.debug(f"Completed Spike Job Arrival: {min_work_spike_job.arrival_time}")

                    # Aggiorno il tempo al prossimo arrivo
                    time_to_next_arrival -= time_to_next_event

                    # Aggiorno le statistiche
                    if t > Simulator.BIAS_PHASE: 
                        area.completed_spike += 1
                        #TODO
                elif time_to_next_event == time_to_next_arrival:
                    # Arrivo di un nuovo job
                    is_spike = (len(web_jobs) >= SI_max)
                    service_demand = self._GetServiceSpike(spike_stream) if is_spike else self._GetServiceWeb(web_stream)
                    new_job = Simulator.Job(t, service_demand, is_spike)
                    if is_spike:
                        if len(spike_jobs) == 0:
                            area.scaling_actions += 1
                        spike_jobs.append(new_job)
                    else:
                        web_jobs.append(new_job)
                    logging.debug(f"New {'Spike' if is_spike else 'Web'} Job Arrival: {t}, Service Demand: {service_demand}")
                    # Programmo il prossimo arrivo
                    time_to_next_arrival = self._GetArrival(arrival_stream)
                else:
                    # Nessun evento, dovrebbe essere impossibile
                    raise Exception(f"Nessun evento trovato. \n \
                                    Next Time Event: {time_to_next_event}, \n \
                                    Arrival: {time_to_next_arrival}, \n \
                                    Min Web: {time_to_complete_web if len(web_jobs) > 0 else Simulator.INFINITY}, \n \
                                    Min Spike: {time_to_complete_spike if len(spike_jobs) > 0 else Simulator.INFINITY}")
                    
                # Aggiorno il tempo corrente
                t = t + time_to_next_event

            if any(stream_usage > MODULUS / STREAMS for stream_usage in self._stream_usage.values()):
                logging.warning("The use of the RNG stream has exceeded the maximum limit!")
                arrival_stream = 4 * Simulator.N_PROCESSES + worker_id
                web_stream = 5 * Simulator.N_PROCESSES + worker_id
                spike_stream = 6 * Simulator.N_PROCESSES + worker_id
                self._stream_usage = {}
                break

            # # --- Risultati Finali ---
            interval_time = Simulator.STOP - Simulator.BIAS_PHASE
            avg_interarrival = area.area_node_web / area.completed_web if area.completed_web > 0 else 0.0
            total_jobs_completed = area.completed_web + area.completed_spike
            avg_response_time_web = area.area_node_web / area.completed_web if area.completed_web > 0 else None
            avg_response_time_spike = area.area_node_spike / area.completed_spike if area.completed_spike > 0 else None
            avg_response_time_total = (area.area_node_web + area.area_node_spike) / total_jobs_completed if total_jobs_completed > 0 else None
            utilization_web = area.area_busy_web / interval_time
            utilization_spike = area.area_busy_spike / interval_time
            throughput_web = area.completed_web / interval_time
            throughput_spike = area.completed_spike / interval_time
            throughput_total = total_jobs_completed / interval_time
            

            replica_results = {
                "web_response_time": avg_response_time_web,
                "spike_response_time": avg_response_time_spike,
                "total_response_time": avg_response_time_total,
                "utilization_web": utilization_web,
                "utilization_spike": utilization_spike,
                "throughput_web": throughput_web,
                "throughput_spike": throughput_spike,
                "throughput_total": throughput_total,
                "scaling_actions": area.scaling_actions
            }

            queue_output.put(replica_results)
            input_replica = queue_input.get()
            logging.debug(f"Queue Input Replica: {input_replica}")
        logging.debug(f"Worker {worker_id} terminato con stream usage {self._stream_usage}.")

if __name__ == "__main__":
    sim = Simulator()
    parameters, stats = sim.run()
    logging.info("Simulazione completata con i seguenti parametri:")
    SI_max, arrival_mean, web_mean, spike_mean, cv = parameters
    logging.info(f"  SI_max: {SI_max}")
    logging.info(f"  Arrival Mean: {arrival_mean}")
    logging.info(f"  Web Mean: {web_mean}")
    logging.info(f"  Spike Mean: {spike_mean}")
    logging.info(f"  Coefficiente di Variazione: {cv}")
    logging.info("Statistiche raccolte con intervallo di confidenza al 95%:")
    for metric, w in stats.items():
        mean = w.mean
        ci = w.confidence_interval_95()
        logging.info(f"{metric:<25}: {mean:.4f} +/- {ci:.4f} (Var: {w.variance:.6f})")