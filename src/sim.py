from rngs import plantSeeds, selectStream
from hyperexp import Hyperexponential
import logging
import numpy as np
import multiprocessing

# Configurazione logging per debug
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Classi per Statistiche e Tempi ---
class Track:
    def __init__(self):
        self.node_web = 0.0
        self.node_spike = 0.0
        self.completed_web = 0
        self.completed_spike = 0
        self.total_response_time = 0.0



# --- Programma Principale ---
class Simulator:
    START      = 0.0
    BIAS_PHASE = 120.0         # Fase Transitoria di 120 secondi
    STOP       = 1200.0         # Simuliamo fino a 1200 secondi
    INFINITY   = 1e15
    SEED = 8
    REPLICAS = 100
    N_PROCESSES = 15    # Numero di processi paralleli per eseguire le repliche, deve essere <= 85 se no non bastano gli stream RNG

    def __init__(self):
        plantSeeds(Simulator.SEED)
        # --- Parametri del Modello di default ---
        self._SI_max = 10                 # Soglia SI_max (da ottimizzare)
        self._arrival_mean = 0.15          # 400 req/min
        self._web_mean     = 0.16          
        self._spike_mean   = 0.08          # Tasso doppio rispetto al web server 
        self._cv           = 4.0           # Coefficiente di variazione richiesto 
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

    def set_parameters(self, arrival_mean, web_mean, spike_mean, cv):
        self.arrival_mean = arrival_mean
        self.web_mean = web_mean
        self.spike_mean = spike_mean
        self.cv = cv
        
    def reset(self):
        pass

    def reset_seed(self):
        plantSeeds(Simulator.SEED)

    def _GetArrival(self, stream):
        selectStream(stream) # Stream 0 per gli arrivi
        return Hyperexponential(self.arrival_mean, self.cv)

    def _GetServiceWeb(self, stream):
        selectStream(stream) # Stream 1 per il Web Server
        return Hyperexponential(self.web_mean, self.cv)
    
    def _GetServiceSpike(self, stream):
        selectStream(stream) # Stream 2 per lo Spike Server 
        return Hyperexponential(self.spike_mean, self.cv)
    class Job:
        def __init__(self, arrival_time, service_demand, is_spike=False):
            self.arrival_time = arrival_time
            self.service_demand = service_demand
            self.remaining_work = service_demand
            self.is_spike = is_spike

    def run(self):

        queue_input = multiprocessing.Queue()
        queue_output = multiprocessing.Queue()

        for replica in range(Simulator.REPLICAS):
            queue_input.put(replica)
            print(f"Replica #{replica} inviata al queue input")

        for worker in range(Simulator.N_PROCESSES):
            queue_input.put(None)  # Segnali di terminazione
        
        processes = []
        for worker_id in range(Simulator.N_PROCESSES):
            p = multiprocessing.Process(target=self._run, args=(worker_id, queue_input, queue_output, self._SI_max))
            processes.append(p)
            p.start()

        # Processo i risultati intanto
        for i in range(Simulator.REPLICAS):
            result = queue_output.get()
            print(f"Risultati Replica #{i} raccolti")
            # TODO

        for p in processes:
            p.join()

    def _run(self, worker_id, queue_input, queue_output, SI_max):
        plantSeeds(Simulator.SEED)
        arrival_stream = worker_id
        web_stream = Simulator.N_PROCESSES + worker_id
        spike_stream = 2 * Simulator.N_PROCESSES + worker_id

        input_replica = queue_input.get()
        print(f"Queue Input Replica: {input_replica}")
        while input_replica is not None:

            t = Simulator.START
            area = Track()
            t = Simulator.START
            time_to_next_arrival = self._GetArrival(arrival_stream)
            web_jobs = []
            spike_jobs = []
            n_web_completed = 0
            n_spike_completed = 0
            busy_time_web = 0.0
            busy_time_spike = 0.0

            scaling_actions = 0

            response_times_web = []
            response_times_spike = []

        
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

                # A questo punto aggiorno il tempo rimanente di lavoro per ogni job in servizio in base al tempo trascorso
                for job in web_jobs:
                    job.remaining_work -= time_to_next_event / len(web_jobs)  # Condivisione della CPU tra i job
                for job in spike_jobs:
                    job.remaining_work -= time_to_next_event / len(spike_jobs)  # Condivisione della CPU tra i job

                if len(web_jobs) > 0:
                    logging.debug("Updated Remaining Work for Web Jobs:")
                    for job in web_jobs:
                        logging.debug(f"  Job Arrival: {job.arrival_time}, Remaining Work: {job.remaining_work}")
                    
                    busy_time_web += time_to_next_event 

                if len(spike_jobs) > 0:
                    logging.debug("Updated Remaining Work for Spike Jobs:")
                    for job in spike_jobs:
                        logging.debug(f"  Job Arrival: {job.arrival_time}, Remaining Work: {job.remaining_work}")

                    busy_time_spike += time_to_next_event

                # A questo punto processo l'evento
                if min_work_web_job is not None and time_to_complete_web == time_to_next_event:
                    # Completamento di un job, lo leviamo dalla lista 
                    web_jobs.remove(min_work_web_job)
                    logging.debug(f"Completed Web Job Arrival: {min_work_web_job.arrival_time}")

                    # Aggiorno il tempo al prossimo arrivo
                    time_to_next_arrival -= time_to_next_event
                    
                    # Aggiorno le statistiche
                    n_web_completed += 1
                    if t > Simulator.BIAS_PHASE: response_times_web.append(t + time_to_next_event - min_work_web_job.arrival_time)
                    #TODO
                elif min_work_spike_job is not None and time_to_complete_spike == time_to_next_event:
                    # Completamento di un job, lo leviamo dalla lista 
                    spike_jobs.remove(min_work_spike_job)
                    logging.debug(f"Completed Spike Job Arrival: {min_work_spike_job.arrival_time}")

                    # Aggiorno il tempo al prossimo arrivo
                    time_to_next_arrival -= time_to_next_event

                    # Aggiorno le statistiche
                    n_spike_completed += 1
                    if t > Simulator.BIAS_PHASE: response_times_spike.append(t + time_to_next_event - min_work_spike_job.arrival_time)
                    #TODO
                elif time_to_next_event == time_to_next_arrival:
                    # Arrivo di un nuovo job
                    is_spike = (len(web_jobs) >= SI_max)
                    service_demand = self._GetServiceSpike(spike_stream) if is_spike else self._GetServiceWeb(web_stream)
                    new_job = Simulator.Job(t, service_demand, is_spike)
                    if is_spike:
                        if len(spike_jobs) == 0:
                            scaling_actions += 1
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

            # # --- Risultati Finali ---
            total_jobs_completed = n_web_completed + n_spike_completed
            np_response_times_web = np.array(response_times_web)
            np_response_times_spike = np.array(response_times_spike)
            logging.debug(f"np_response_times_web: {np_response_times_web[0:10]}")
            logging.debug(f"np_response_times_spike: {np_response_times_spike[0:10]}")
            avg_response_time_web = np_response_times_web.mean()
            avg_response_time_spike = np_response_times_spike.mean()
            np_response_times_total = np.concatenate((np_response_times_web, np_response_times_spike))
            avg_response_time_total = np_response_times_total.mean()
            interval_time = Simulator.STOP - Simulator.BIAS_PHASE

            queue_output.put("Results")

            # print(f"--- Risultati con SI_MAX = {SI_max} ---")
            # print(f"Job Totali: {total_jobs_completed}")
            # print(f"Tempo di Risposta Medio Web (E[R]): {avg_response_time_web:.4f} s")
            # print(f"Tempo di Risposta Medio Spike (E[R]): {avg_response_time_spike:.4f} s")
            # print(f"Tempo di Risposta Medio Totale (E[R]): {avg_response_time_total:.4f} s")
            # print(f"Percentuale Job Web Server: {(n_web_completed/total_jobs_completed)*100:.2f}%")
            # print(f"Percentuale Job Spike Server: {(n_spike_completed/total_jobs_completed)*100:.2f}%")
            # print(f"Utilizzazione Web Server: {(busy_time_web/interval_time)*100:.2f}%")
            # print(f"Utilizzazione Spike Server: {(busy_time_spike/interval_time)*100:.2f}%")
            # print(f"Throughput Web Server: {n_web_completed/ interval_time:.4f} jobs/s")
            # print(f"Throughput Spike Server: {n_spike_completed/ interval_time:.4f} jobs/s")
            # print(f"Throughput Totale: {total_jobs_completed/ interval_time:.4f} jobs/s")
            # print(f"Numero di Azioni di Scaling: {scaling_actions}")
            input_replica = queue_input.get()
            print(f"Queue Input Replica: {input_replica}")
        print(f"Worker {worker_id} terminato.")

if __name__ == "__main__":
    sim = Simulator()
    sim.run()