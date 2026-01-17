from rngs import plantSeeds, selectStream
from hyperexp import Hyperexponential
import logging
import numpy as np

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

    def __init__(self):
        plantSeeds(8)
        # --- Parametri del Modello di default ---
        self._SI_max = 160                 # Soglia SI_max (da ottimizzare)
        self._arrival_mean = 0.15          # 400 req/min
        self._web_mean     = 0.16          
        self._spike_mean   = 0.08          # Tasso doppio rispetto al web server 
        self._cv           = 4.0           # Coefficiente di variazione richiesto 

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
        self._area = Track()
        self._t = Simulator.START
        self._time_to_next_arrival = self._GetArrival()
        self._web_jobs = []
        self._spike_jobs = []
        self._n_web_completed = 0
        self._n_spike_completed = 0
        self._busy_time_web = 0.0
        self._busy_time_spike = 0.0

        self._in_spike_mode = False
        self._scaling_actions = 0

        self._response_times_web = []
        self._response_times_spike = []

    

    def _GetArrival(self):
        selectStream(0) # Stream 0 per gli arrivi
        return Hyperexponential(self.arrival_mean, self.cv)

    def _GetServiceWeb(self):
        selectStream(1) # Stream 1 per il Web Server
        return Hyperexponential(self.web_mean, self.cv)
    def _GetServiceSpike(self):
        selectStream(2) # Stream 2 per lo Spike Server 
        return Hyperexponential(self.spike_mean, self.cv)

    class Job:
        def __init__(self, arrival_time, service_demand, is_spike=False):
            self.arrival_time = arrival_time
            self.service_demand = service_demand
            self.remaining_work = service_demand
            self.is_spike = is_spike

    def run(self):
        while (t < self.STOP):
            # Per trovare il prossimo evento devo vedere chi è che il tempo di completamento più piccolo e confrontarlo con il prossimo arrivo
            logging.debug(f"Current Time: {t}")

            logging.debug(f"Next Arrival Event at: {time_to_next_arrival}")

            min_work_web_job = None
            min_work_spike_job = None

            if len(self._web_jobs) > 0:
                # trovo il job con il minimo lavoro rimanente nel web server
                min_work_web_job = min(self._web_jobs, key=lambda job: job.remaining_work)
                time_to_complete_web = min_work_web_job.remaining_work * len(self._web_jobs)
                logging.debug(f"Time to Complete Web Job: {time_to_complete_web} (Remaining Work: {min_work_web_job.remaining_work})")
            if len(self._spike_jobs) > 0:
                # trovo il job con il minimo lavoro rimanente nello spike server
                min_work_spike_job = min(self._spike_jobs, key=lambda job: job.remaining_work)
                time_to_complete_spike = min_work_spike_job.remaining_work * len(self._spike_jobs)
                logging.debug(f"Time to Complete Spike Job: {time_to_complete_spike} (Remaining Work: {min_work_spike_job.remaining_work})")

            time_to_next_event = min(
                time_to_next_arrival,
                time_to_complete_web if len(self._web_jobs) > 0 else Simulator.INFINITY,
                time_to_complete_spike if len(self._spike_jobs) > 0 else Simulator.INFINITY
            )
            logging.debug(f"Next Time Event at: {time_to_next_event}")

            # A questo punto aggiorno il tempo rimanente di lavoro per ogni job in servizio in base al tempo trascorso
            for job in self._web_jobs:
                job.remaining_work -= time_to_next_event / len(self._web_jobs)  # Condivisione della CPU tra i job
            for job in self._spike_jobs:
                job.remaining_work -= time_to_next_event / len(self._spike_jobs)  # Condivisione della CPU tra i job

            if len(self._web_jobs) > 0:
                logging.debug("Updated Remaining Work for Web Jobs:")
                for job in self._web_jobs:
                    logging.debug(f"  Job Arrival: {job.arrival_time}, Remaining Work: {job.remaining_work}")
                
                self._busy_time_web += time_to_next_event 

            if len(self._spike_jobs) > 0:
                logging.debug("Updated Remaining Work for Spike Jobs:")
                for job in self._spike_jobs:
                    logging.debug(f"  Job Arrival: {job.arrival_time}, Remaining Work: {job.remaining_work}")

                self._busy_time_spike += time_to_next_event

            # A questo punto processo l'evento
            if min_work_web_job is not None and time_to_complete_web == time_to_next_event:
                # Completamento di un job, lo leviamo dalla lista 
                self._web_jobs.remove(min_work_web_job)
                logging.debug(f"Completed Web Job Arrival: {min_work_web_job.arrival_time}")

                # Aggiorno il tempo al prossimo arrivo
                time_to_next_arrival -= time_to_next_event
                
                # Aggiorno le statistiche
                n_web_completed += 1
                if t > Simulator.BIAS_PHASE: self._response_times_web.append(t + time_to_next_event - min_work_web_job.arrival_time)
                #TODO
            elif min_work_spike_job is not None and time_to_complete_spike == time_to_next_event:
                # Completamento di un job, lo leviamo dalla lista 
                self._spike_jobs.remove(min_work_spike_job)
                logging.debug(f"Completed Spike Job Arrival: {min_work_spike_job.arrival_time}")

                if(len(self._spike_jobs) == 0):
                    in_spike_mode = False

                # Aggiorno il tempo al prossimo arrivo
                time_to_next_arrival -= time_to_next_event

                # Aggiorno le statistiche
                n_spike_completed += 1
                if t > Simulator.BIAS_PHASE: self._response_times_spike.append(t + time_to_next_event - min_work_spike_job.arrival_time)
                #TODO
            elif time_to_next_event == time_to_next_arrival:
                # Arrivo di un nuovo job
                is_spike = (len(self._web_jobs) >= self._SI_max)
                service_demand = self._GetServiceSpike() if is_spike else self._GetServiceWeb()
                new_job = Simulator.Job(t, service_demand, is_spike)
                if is_spike:
                    if len(self._spike_jobs) == 0:
                        in_spike_mode = True
                        scaling_actions += 1
                    self._spike_jobs.append(new_job)
                else:
                    self._web_jobs.append(new_job)
                logging.debug(f"New {'Spike' if is_spike else 'Web'} Job Arrival: {t}, Service Demand: {service_demand}")
                # Programmo il prossimo arrivo
                time_to_next_arrival = self._GetArrival()
            else:
                # Nessun evento, dovrebbe essere impossibile
                raise Exception(f"Nessun evento trovato. \n \
                                Next Time Event: {time_to_next_event}, \n \
                                Arrival: {time_to_next_arrival}, \n \
                                Min Web: {time_to_complete_web if len(self._web_jobs) > 0 else Simulator.INFINITY}, \n \
                                Min Spike: {time_to_complete_spike if len(self._spike_jobs) > 0 else Simulator.INFINITY}")
                
            # Aggiorno il tempo corrente
            t = t + time_to_next_event
        # # --- Risultati Finali ---
        total_jobs_completed = n_web_completed + n_spike_completed
        np_response_times_web = np.array(self._response_times_web)
        np_response_times_spike = np.array(self._response_times_spike)
        avg_response_time_web = np_response_times_web.mean()
        avg_response_time_spike = np_response_times_spike.mean()
        np_response_times_total = np.concatenate((np_response_times_web, np_response_times_spike))
        avg_response_time_total = np_response_times_total.mean()
        interval_time = Simulator.STOP - Simulator.BIAS_PHASE

        print(f"--- Risultati con SI_MAX = {self._SI_max} ---")
        print(f"Job Totali: {total_jobs_completed}")
        print(f"Tempo di Risposta Medio Web (E[R]): {avg_response_time_web:.4f} s")
        print(f"Tempo di Risposta Medio Spike (E[R]): {avg_response_time_spike:.4f} s")
        print(f"Tempo di Risposta Medio Totale (E[R]): {avg_response_time_total:.4f} s")
        print(f"Percentuale Job Web Server: {(n_web_completed/total_jobs_completed)*100:.2f}%")
        print(f"Percentuale Job Spike Server: {(n_spike_completed/total_jobs_completed)*100:.2f}%")
        print(f"Utilizzazione Web Server: {(self._busy_time_web/interval_time)*100:.2f}%")
        print(f"Utilizzazione Spike Server: {(self._busy_time_spike/interval_time)*100:.2f}%")
        print(f"Throughput Web Server: {n_web_completed/ interval_time:.4f} jobs/s")
        print(f"Throughput Spike Server: {n_spike_completed/ interval_time:.4f} jobs/s")
        print(f"Throughput Totale: {total_jobs_completed/ interval_time:.4f} jobs/s")
        print(f"Numero di Azioni di Scaling: {scaling_actions}")