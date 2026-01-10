# ------------------------------------------------------------------------- 
# * This program is a next-event simulation of a single-server FIFO service
# * node using Hyperexponentially distributed interarrival times and 
# * Hyperexponentially distributed service times (i.e., a H/H/1 queue).  
# * The service node is assumed to be initially idle, no arrivals are 
# * permitted after the terminal time STOP, and the node is then purged 
# * by processing any remaining jobs in the service node.
# *
# * Name            : ssq_hyper.py  (Single Server Queue, Hyperexponential)
# * Author          : Steve Park & Dave Geyer (original)
# * Language        : Python 3
# * Latest Revision : Modified for Hyperexponential distributions
# * ------------------------------------------------------------------------- 
# */

from rngs import selectStream, plantSeeds
from rvgs import Exponential, Erlang
from hyperexp import Hyperexponential

START =        0.0              # initial time                   */
STOP  =    20000.0              # terminal (close the door) time */
INFINITY =  (100.0 * STOP)      # must be much larger than STOP  */
arrivalTemp = START 


MEAN_ARRIVAL = 0.15            # mean interarrival time         
CV_ARRIVAL = 4.0               # coefficient of variation (arrival)
MEAN_SERVICE = 0.15            # mean service time               
CV_SERVICE = 4.0               # coefficient of variation (service)

def Min(a,c):
# ------------------------------
# * return the smaller of a, b
# * ------------------------------
# */
  if (a < c):
    return (a)
  else:
    return (c)


def GetArrival():
# ---------------------------------------------
# * generate the next arrival time using a 
# * Hyperexponential distribution with given
# * mean and coefficient of variation
# * --------------------------------------------- 
  global arrivalTemp

  selectStream(0) 
  arrivalTemp += Hyperexponential(MEAN_ARRIVAL,CV_ARRIVAL)
  return (arrivalTemp)



def GetService():
# --------------------------------------------
# * generate the next service time using a 
# * Hyperexponential distribution with given
# * mean and coefficient of variation
# * -------------------------------------------- 
  selectStream(1)
  return (Hyperexponential(MEAN_SERVICE,CV_SERVICE))

class track:
  node = 0.0                   # time integrated number in the node  */
  queue = 0.0                  # time integrated number in the queue */
  service = 0.0                # time integrated number in service   */

class time:
  arrival = -1                # next arrival time                   */
  completion = -1             # next completion time                */
  current  = -1               # current time                        */
  next = -1                   # next (most imminent) event time     */
  last = -1                   # last arrival time                   */  

###############################Main Program##############################


index  = 0                  # used to count departed jobs         */
number = 0                  # number in the node                  */
area = track()
t = time()

plantSeeds(123456789)

t.current    = START           # set the clock                         */
t.arrival    = GetArrival()    # schedule the first arrival            */
t.completion = INFINITY        # the first event can't be a completion */

while (t.arrival < STOP) or (number > 0):
  t.next          = Min(t.arrival, t.completion)  # next event time   */
  if (number > 0):                               # update integrals  */
    area.node    += (t.next - t.current) * number
    area.queue   += (t.next - t.current) * (number - 1)
    area.service += (t.next - t.current)
  #EndIf

  t.current       = t.next                    # advance the clock */

  if (t.current == t.arrival):               # process an arrival */
    number += 1
    t.arrival     = GetArrival()
    if (t.arrival > STOP):
      t.last      = t.current
      t.arrival   = INFINITY

    if (number == 1):
      t.completion = t.current + GetService()
  #EndOuterIf
  else:                                        # process a completion */
    index += 1
    number -= 1
    if (number > 0):
      t.completion = t.current + GetService()
    else:
      t.completion = INFINITY
  
#EndWhile 

print("\nfor {0} jobs".format(index))
print("   average interarrival time = {0:6.2f}".format(t.last / index))
print("   average wait ............ = {0:6.2f}".format(area.node / index))
print("   average delay ........... = {0:6.2f}".format(area.queue / index))
print("   average service time .... = {0:6.2f}".format(area.service / index))
print("   average # in the node ... = {0:6.2f}".format(area.node / t.current))
print("   average # in the queue .. = {0:6.2f}".format(area.queue / t.current))
print("   utilization ............. = {0:6.2f}".format(area.service / t.current))

# C output:
# Enter a positive integer seed (9 digits or less) >> 123456789

# for 10025 jobs
#    average interarrival time =   1.99
#    average wait ............ =   4.11
#    average delay ........... =   2.62
#    average service time .... =   1.50
#    average # in the node ... =   2.06
#    average # in the queue .. =   1.31
#    utilization ............. =   0.75