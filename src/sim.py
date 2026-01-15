from rngs import plantSeeds, selectStream
from hyperexp import Hyperexponential
import logging
import numpy as np

# Configurazione logging per debug
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Parametri del Modello ---
START      = 0.0
TRANSIENT_PHASE = 120.0     # Fase transitoria di 120 secondi
STOP       = 1200.0         # Simuliamo fino a 1200 secondi
SI_MAX     = 0             # Soglia SI_max (da ottimizzare)
INFINITY   = 1e15

# Specifiche Carico (Media in secondi)
ARRIVAL_MEAN = 0.15          # 400 req/min
WEB_MEAN     = 0.16          
SPIKE_MEAN   = 0.08          # Tasso doppio rispetto al web server 
CV           = 4.0           # Coefficiente di variazione richiesto 


def GetArrival():
    selectStream(0) # Stream 0 per gli arrivi
    return Hyperexponential(ARRIVAL_MEAN, CV)

def GetServiceWeb():
    selectStream(1) # Stream 1 per il Web Server
    return Hyperexponential(WEB_MEAN, CV)

def GetServiceSpike():
    selectStream(2) # Stream 2 per lo Spike Server 
    return Hyperexponential(SPIKE_MEAN, CV)

# --- Classi per Statistiche e Tempi ---
class Track:
    def __init__(self):
        self.node_web = 0.0
        self.node_spike = 0.0
        self.completed_web = 0
        self.completed_spike = 0
        self.total_response_time = 0.0

class Job:
    def __init__(self, arrival_time, service_demand, is_spike=False):
        self.arrival_time = arrival_time
        self.service_demand = service_demand
        self.remaining_work = service_demand
        self.is_spike = is_spike

# --- Programma Principale ---
area = Track()
t = START
time_to_next_arrival = GetArrival()
web_jobs = []
spike_jobs = []
n_web_completed = 0
n_spike_completed = 0
busy_time_web = 0.0
busy_time_spike = 0.0

in_spike_mode = False
scaling_actions = 0

response_times_web = []
response_times_spike = []

plantSeeds(123456789)


while (t < STOP):
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
        time_to_complete_web if len(web_jobs) > 0 else INFINITY,
        time_to_complete_spike if len(spike_jobs) > 0 else INFINITY
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
        if t > TRANSIENT_PHASE: response_times_web.append(t + time_to_next_event - min_work_web_job.arrival_time)
        #TODO
    elif min_work_spike_job is not None and time_to_complete_spike == time_to_next_event:
        # Completamento di un job, lo leviamo dalla lista 
        spike_jobs.remove(min_work_spike_job)
        logging.debug(f"Completed Spike Job Arrival: {min_work_spike_job.arrival_time}")

        if(len(spike_jobs) == 0):
            in_spike_mode = False

        # Aggiorno il tempo al prossimo arrivo
        time_to_next_arrival -= time_to_next_event

        # Aggiorno le statistiche
        n_spike_completed += 1
        if t > TRANSIENT_PHASE: response_times_spike.append(t + time_to_next_event - min_work_spike_job.arrival_time)
        #TODO
    elif time_to_next_event == time_to_next_arrival:
        # Arrivo di un nuovo job
        is_spike = (len(web_jobs) >= SI_MAX)
        service_demand = GetServiceSpike() if is_spike else GetServiceWeb()
        new_job = Job(t, service_demand, is_spike)
        if is_spike:
            if len(spike_jobs) == 0:
                in_spike_mode = True
                scaling_actions += 1
            spike_jobs.append(new_job)
        else:
            web_jobs.append(new_job)
        logging.debug(f"New {'Spike' if is_spike else 'Web'} Job Arrival: {t}, Service Demand: {service_demand}")
        # Programmo il prossimo arrivo
        time_to_next_arrival = GetArrival()
    else:
        # Nessun evento, dovrebbe essere impossibile
        raise Exception(f"Nessun evento trovato. \n \
                        Next Time Event: {time_to_next_event}, \n \
                        Arrival: {time_to_next_arrival}, \n \
                        Min Web: {time_to_complete_web if len(web_jobs) > 0 else INFINITY}, \n \
                        Min Spike: {time_to_complete_spike if len(spike_jobs) > 0 else INFINITY}")
        
    # Aggiorno il tempo corrente
    t = t + time_to_next_event
# # --- Risultati Finali ---
total_jobs_completed = n_web_completed + n_spike_completed
np_response_times_web = np.array(response_times_web)
np_response_times_spike = np.array(response_times_spike)
avg_response_time_web = np_response_times_web.mean()
avg_response_time_spike = np_response_times_spike.mean()
np_response_times_total = np.concatenate((np_response_times_web, np_response_times_spike))
avg_response_time_total = np_response_times_total.mean()

print(f"--- Risultati con SI_MAX = {SI_MAX} ---")
print(f"Job Totali: {total_jobs_completed}")
print(f"Tempo di Risposta Medio Web (E[R]): {avg_response_time_web:.4f} s")
print(f"Tempo di Risposta Medio Spike (E[R]): {avg_response_time_spike:.4f} s")
print(f"Tempo di Risposta Medio Totale (E[R]): {avg_response_time_total:.4f} s")
print(f"Percentuale Job Web Server: {(n_web_completed/total_jobs_completed)*100:.2f}%")
print(f"Percentuale Job Spike Server: {(n_spike_completed/total_jobs_completed)*100:.2f}%")
print(f"Utilizzazione Web Server: {(busy_time_web/t)*100:.2f}%")
print(f"Utilizzazione Spike Server: {(busy_time_spike/t)*100:.2f}%")
print(f"Throughput Web Server: {n_web_completed/ t:.4f} jobs/s")
print(f"Throughput Spike Server: {n_spike_completed/ t:.4f} jobs/s")
print(f"Throughput Totale: {total_jobs_completed/ t:.4f} jobs/s")
print(f"Numero di Azioni di Scaling: {scaling_actions}")