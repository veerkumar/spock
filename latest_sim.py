import time
import logging
import math
import random
from multiprocessing import Queue
import Queue
import copy
import collections
import sys
import os
#import numpypy
import numpy as np
from numpy import mean
from collections import defaultdict
import pandas as pd
from sklearn.linear_model import LinearRegression
INITIAL_WORKERS = 10
VM_PREDICTION = 0
load_tracking = 0
MONITOR_INTERVAL = int(100000)
start_up_delay = int(100000)
last_task = 0
def get_percentile(N, percent, key=lambda x:x):
    if not N:
        return 0
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0 + d1

def plot_cdf(values, filename):
    values.sort()
    f = open('filename', "w")
    for percent in range(100):
        fraction = percent / 100.
        f.write("%s\t%s\n" % (fraction, get_percentile(values, fraction)))
    f.close()

def lambda_cost(num, mem, time):
    return float(num*0.0000002 + (num*mem*time*0.00001667))

class TaskEndEvent:

    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        #print "task end event"
        return self.worker.free_slot(current_time)

class Lambda(object):
    def __init__(
            self,
            simulation,
            current_time,
            up_time,
            task_type
            ):
        self.simulation = simulation
        self.start_time = current_time
        self.up_time = up_time
        if task_type == 0:
            self.exec_time = float(400)
            self.mem = float(2024)
        if task_type == 1:
            self.exec_time = float(400)
            self.mem = float(3024)
        if task_type == 2:
            self.exec_time = float(950)
            self.mem = float(3048)
    def execute_task(self,task_id, current_time):
        #print("executing task in lambda")
        task_duration = self.simulation.tasks[task_id].exec_time
        probe_response_time = 5 + current_time
        task = self.simulation.tasks[task_id]
        if task_duration > 0:
            task_end_time = task_duration + probe_response_time
            print >> tasks_file,"task_id ,", task.id, ",", "task_type," ,task.task_type,  ",",  "lambda task" , ",task_end_time ,", task_end_time, ",", "task_start_time,",task.start_time, ",", " each_task_running_time,",(task_end_time - task.start_time),",", " task_queuing_time:,", (task_end_time - task.start_time) - task.exec_time

        if(self.simulation.add_task_completion_time(task_id,
            task_end_time,1)):

            print >> finished_file,"num_tasks ,", task.num_tasks, "," ,"VM_tasks ,", task.vm_tasks,"lambda_tasks ,", task.lambda_tasks ,"task_end_time, ", task_end_time,",", "task_start_time,",task.start_time,",", " each_task_running_time, ",(task.end_time - task.start_time)

            new_event = TaskEndEvent(self)
            return [(task_end_time, new_event)]
        return []
    def free_slot(self, current_time):
        #self.free_slots += 1
        #get_task_events = self.get_task(current_time)
        return []


class VM(object):

    vm_count = 1

    def __init__(
            self,
            simulation,
            current_time,
            up_time,
            task_type,
            vcpu,
            vmem,
            price,
            spin_up,
            id):
        self.simulation = simulation
        self.start_time = current_time
        self.up_time = up_time
        self.end_time = current_time
        self.vcpu = vcpu
        self.vmem = vmem
        self.queued_tasks = Queue.PriorityQueue()
        self.id = id
        self.isIdle = True
        self.lastIdleTime = current_time
        self.price = price
        self.task_type = task_type
        self.spin_up = spin_up
        #print("adding worker id and type",self.id,self.task_type)
        if task_type == 0:
            self.free_slots = 6
            self.max_slots = 6
        if task_type == 1:
            self.free_slots = 5
            self.max_slots = 5
        if task_type == 2:
            self.free_slots = 2
            self.max_slots = 2
        self.num_queued_tasks = 0

    def add_task(self, task_id, current_time):
        #print("adding new task to VM", task_id,self.id)
        self.queued_tasks.put((current_time,task_id))
        self.isIdle = False
        new_events = self.get_task(current_time)
        self.spin_up = False
        # self.probes_repliid_to_immediately += len(new_events)
        # logging.getLogger("sim").debug("Worker %s: %s" %(self.id, self.probes_replied_to_immediately))

        return new_events

    def VM_status(self, current_time):
        if (not self.isIdle and (self.num_queued_tasks == 0)):
            self.isIdle = True
            self.lastIdleTime = current_time
            return True
        return False
    def get_task(self, current_time):
        new_events = []
        #print("running task on worker",self.id,self.task_type)
        if not self.queued_tasks.empty():
            #print "task queued", self.num_queued_tasks, self.queued_tasks.qsize()
            #print("worker not empty at time",self.id,self.task_type,current_time)
            if(self.free_slots == 0):

                #print self.id," task queued delay", self.num_queued_tasks
                (queue_time,task_id) = self.queued_tasks.get()
                #print current_time + self.simulation.tasks[task_id].exec_time + 10, self.simulation.tasks[task_id].start_time
                new_events.append((current_time + self.simulation.tasks[task_id].exec_time + 10,ScheduleVMEvent(self,task_id)))
                return new_events

            #print self.id,self.num_queued_tasks,self.free_slots,"executing task"
            self.free_slots -= 1
            self.num_queued_tasks -= 1
            (queue_time,task_id) = self.queued_tasks.get()
            task_duration = self.simulation.tasks[task_id].exec_time
            probe_response_time = 5 + current_time
            if task_duration > 0:
                task_end_time = task_duration + probe_response_time
                #print("worker not empty at time",self.id,self.task_type,task_end_time)
                new_event = TaskEndEvent(self)
                task = self.simulation.tasks[task_id]
                #if task.id >=15548:
                 #   print ("task id ", task.id, "task type" , "VM id" , self.id, task.task_type, "task_end_time ", task_end_time, "task_start_time:",task.start_time, " each_task_running_time: ",(task_end_time - task.start_time))
                print >> tasks_file,"task_id ,", task.id,",",  "task_type," ,task.task_type, ",", "VM_id," , self.id ,",", "task_end_time ,", task_end_time, ",", "task_start_time,",task.start_time, ",", " each_task_running_time,",(task_end_time - task.start_time), ",", " task_queuing_time:,", (task_end_time - task.start_time) - task.exec_time
                if(self.simulation.add_task_completion_time(task_id,
                    task_end_time,0)):
                    #print "writing to file"
                    print >> finished_file,"num tasks ", task.num_tasks, "," ,"VM_tasks ,", task.vm_tasks,"lambda_tasks ,", task.lambda_tasks , "task_end_time, ", task_end_time, "task_start_time,",task.start_time, " each_task_running_time ,",(task.end_time - task.start_time)
                return [(task_end_time, new_event)]
        return []

    def free_slot(self, current_time):
        self.free_slots += 1
        #get_task_events = self.get_task(current_time)
        return []

class Task(object):

    task_count = 0

    def __init__(
            self,
            start_time,
            num_tasks,
            task_type,
            ):
        self.id = int(Task.task_count)
        Task.task_count += 1
        self.start_time = start_time
        self.num_tasks = num_tasks
        self.task_type = task_type
        self.end_time = start_time
        self.completed_tasks = 0
        self.lambda_tasks = 0
        self.vm_tasks = 0
        if task_type == 0:
            self.exec_time = 400
            self.mem = 1024
        if task_type == 1:
            self.exec_time = 400
            self.mem = 2024
        if task_type == 2:
            self.exec_time = 950
            self.mem = 3048

    def task_completed(self, completion_time):
        self.completed_tasks += 1
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks <= self.num_tasks
        return self.num_tasks == self.completed_tasks


class Event(object):

    """ Abstract class representing events. """

    def __init__(self):
        raise NotImplementedError('Event is an abstract class and cannot be instantiated directly'
                )

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """

        raise NotImplementedError('The run() method must be implemented by each class subclassing Event'
                )


class TaskArrival(Event):
    """ Event to signify a job arriving at a scheduler. """
    def __init__(
            self,
            simulation,
            interarrival_delay,
            num_tasks,
            task_type,
            ):
        self.simulation = simulation
        self.interarrival_delay = float(interarrival_delay)
        self.num_tasks = int(num_tasks)
        self.task_type = int(task_type)

        # self.task_distribution= task_distribution

    def run(self, current_time):
        global last_task
        task = Task(current_time, self.num_tasks, self.task_type)
        logging.getLogger('sim').debug('Job %s arrived at %s'
                % (task.id, current_time))

        # Schedu1le job.

        new_events = self.simulation.send_tasks(task, current_time)

        # Add new Job Arrival event, for the next job to arrive after this one.

        arrival_delay = random.expovariate(1.0
                / self.interarrival_delay)

        # new_events.append((current_time + arrival_delay, self))
        #print("adding self task arriva",new_events)

        logging.getLogger('sim').debug('Retuning %s events'
                % len(new_events))
        line = self.simulation.tasks_file.readline()
        #print line
        if line == '':
            print('task empty')
            last_task = 1
            return new_events
        start_time = float(line.split(',')[0])
        num_tasks = line.split(',')[3]
        task_type = line.split(',')[2]

        #print "adding new task",int(start_time*1000), num_tasks, task_type
        # new_task = Task(self, line, 1000, start_time, num_tasks, task_type)

        new_events.append((start_time * 1000,
            TaskArrival(self.simulation, start_time
                * 1000, num_tasks, task_type)))
        #print ("task arrival new events", new_events)
        return new_events

class VMCreateEvent(Event):
    def __init__(self,simulation, VM, task_type):
        self.simulation = simulation
        self.VM = VM
        self.task_type = task_type
    def run(self, current_time):
        #self.VMs[self.task_type].append(VM(self.simulation,current_time,60000,self.task_type,4,8192,0.10,True,len(self.VMs[self.task_type])))
        print " spin up compeleted for VM", self.VM.id, self.task_type
        self.VM.spin_up = False
        new_events = []
        return new_events

class VM_Monitor_Event(Event):
    def __init__(self, simulation):
        self.simulation = simulation
    def run(self, current_time):
        new_events = []
        global last_task
        print("M_monito_EVENT")
        for index in range(3):
            width = len(self.simulation.VMs[index])
            k=0
            print( len(self.simulation.VMs[index]), len(self.simulation.completed_VMs[index]))
            while k < width:
                if (not self.simulation.VMs[index][k].spin_up):
                    if(not self.simulation.VMs[index][k].VM_status(current_time) and self.simulation.VMs[index][k].isIdle):
                        if ((current_time - self.simulation.VMs[index][k].lastIdleTime) > 180000):
                        #if(current_time - self.simulation.VMs[index][k].start_time >=3600000):
                            self.simulation.completed_VMs.setdefault(index,[]).append(self.simulation.VMs[index][k])
                            self.simulation.VMs[index][k].end_time = current_time
                            print >> f, self.simulation.VMs[index][k].id, ",",self.simulation.VMs[index][k].end_time,",", self.simulation.VMs[index][k].start_time,",", self.simulation.VMs[index][k].lastIdleTime
                            del self.simulation.VMs[index][k]
                            print index, len(self.simulation.completed_VMs[index]),"width changing"
                        width-=1
                k+=1
        if(self.simulation.event_queue.qsize() > 1 and last_task==0):
            new_events.append((current_time+180000,VM_Monitor_Event(self.simulation)))
        return new_events

class PeriodicTimerEvent(Event):
    def __init__(self,simulation):
        self.simulation= simulation

    def run(self, current_time):
        new_events = []
        global last_task
        print("periodic timer event",current_time,"VM1 VM2 VM3",len(self.simulation.VMs[0]),len(self.simulation.VMs[1]),len(self.simulation.VMs[2]))
      #  total_load       = str(int(10000*(1-self.simulation.total_free_slots*1.0/(TOTAL_WORKERS*SLOTS_PER_WORKER)))/100.0)
      #  small_load       = str(int(10000*(1-self.simulation.free_slots_small_partition*1.0/len(self.simulation.small_partition_workers)))/100.0)
      #  big_load         = str(int(10000*(1-self.simulation.free_slots_big_partition*1.0/len(self.simulation.big_partition_workers)))/100.0)
       # small_not_big_load ="N/A"
       # if(len(self.simulation.small_not_big_partition_workers)!=0):
            #Load        = str(int(10000*(1-self.simulation.free_slots_small_not_big_partition*1.0/len(self.simulation.small_not_big_partition_workers)))/100.0)
        if (load_tracking == 1):
            for i in range(3):
                low_load = 0
                for j in range (len(self.simulation.VMs[i])):
                    if((float(self.simulation.VMs[i][j].num_queued_tasks)/float(self.simulation.VMs[i][j].max_slots)) <=0.4):
                        low_load+=1
                print >> load_file,"VM type," + str(i) + "low_load: "+ str(low_load) + ",num_vms," + str(len(self.simulation.VMs[i])) + ",current_time: " + str(current_time)
                print "load written", i, low_load, len(self.simulation.VMs[i]), float(self.simulation.VMs[i][0].free_slots),self.simulation.VMs[i][0].num_queued_tasks
        if(VM_PREDICTION == 1):
            for i in range(3):
                #print self.simulation.task_arrival[i]
                df = pd.DataFrame((self.simulation.task_arrival[i]),columns=['time','req'])
                train = df[-500:]
                X = train.time.tolist()
                y = train.req.tolist()
                X = np.array(X).reshape(-1,1)
                #print X
                y = np.array(y).reshape(-1,1)
                #print y
                model = LinearRegression()
                model.fit(X, y)
                #print model
                X_predict = np.array([current_time+start_up_delay]).reshape(-1,1)
                y_predict = burst_threshold * model.predict([X_predict[0]])
                print "predicted requests",int(y_predict),"VMs",int(y_predict)/self.simulation.VMs[i][0].max_slots
                if(int(y_predict) > len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots):
                    print ("rolling mean more",y_predict, "existing VM size", len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots)
                    num_vms = int(math.ceil(int(int(y_predict)- len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots)/int(self.simulation.VMs[i][0].max_slots)))
                    if(num_vms !=0):
                        print ("num_vms spawning",burst_threshold*num_vms)
                        for j in range(num_vms):
                        #print "adding new VMs", num_vms
                            self.simulation.VMs[i].append(VM(self.simulation,current_time,start_up_delay,i,4,8192,0.10,True,len(self.simulation.VMs[i])))
                            new_events.append((current_time+start_up_delay,VMCreateEvent(self.simulation,self.simulation.VMs[i][-1],i)))
 #rolling_mean = df.rolling(window=60).mean()
                #rolling_mean = mean([self.simulation.task_arrival[i] for i in range(len(self.simulation.task_arrival[i])-10, len(self.simulation.task_arrival[i]))])
                """rolling_mean = np.mean(np.array(self.simulation.task_arrival[i][-10:]))
                if(rolling_mean > len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots):
                    print "rolling mean more",rolling_mean, "existing VM size", len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots
                    num_vms = int(math.ceil(int(rolling_mean - len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots)/int(self.simulation.VMs[i][0].max_slots)))
                    for j in range(num_vms*2):
                        #print "adding new VMs", num_vms
                        new_events.append((current_time,VMCreateEvent(self.simulation,i)))
                if(rolling_mean < len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots):
                    num_vms = int(math.ceil(int(len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots - rolling_mean)/int(self.simulation.VMs[i][0].max_slots)))
                    print "rolling mean lesser ",rolling_mean, "existing VM size", len(self.simulation.VMs[i])*self.simulation.VMs[i][0].max_slots, "size to be reduced ", num_vms
                    if(num_vms!=0):
                        for j in range(1,num_vms+1):
                            self.simulation.VMs[i][-j].end_time = current_time
                            print self.simulation.VMs[i][-j].start_time, current_time
                        self.simulation.completed_VMs.setdefault(i, []).extend(self.simulation.VMs[i][-num_vms:])
                        self.simulation.VMs[i] = self.simulation.VMs[i][:-num_vms]
                    #    print j,len(self.simulation.VMs[i])
                    #    print j,len(self.simulation.completed_VMs[i])"""

        if(self.simulation.event_queue.qsize() >1 and last_task==0):
            #print "events left ",self.simulation.event_queue.qsize()," tasks arrived", len(self.simulation.task_arrival)
            new_events.append((float(current_time) + MONITOR_INTERVAL,PeriodicTimerEvent(self.simulation)))
        return new_events
class ScheduleVMEvent(Event):

    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id
        self.worker.num_queued_tasks += 1
    def run(self, current_time):
        logging.getLogger('sim'
                ).debug('Probe for job %s arrived at worker %s at %s'
                        % (self.job_id, self.worker.id,
                            current_time))
        return self.worker.add_task(self.job_id, current_time)

class ScheduleLambdaEvent(Event):

    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id
    def run(self, current_time):
        logging.getLogger('sim'
                ).debug('Probe for job %s arrived at %s'
                        % (self.job_id,
                            current_time))
        return self.worker.execute_task(self.job_id, current_time)


class Simulation(object):

    def __init__(self, workload_file):
        self.workload_file = workload_file

        # avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        # self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        # print ("Interarrival delay: %s (avg slots in use: %s)" %
         #      (self.interarrival_delay, avg_used_slots))

        self.tasks = defaultdict()
        self.task_arrival = defaultdict(list)
        # self. num_jobs

        self.event_queue = Queue.PriorityQueue()
        self.VMs = defaultdict(lambda: np.ndarray(0))
        self.completed_VMs = defaultdict(list)
        self.lambdas = defaultdict()
        # self.file_prefix = file_prefix

        j = 0
        while j < INITIAL_WORKERS:
            i = 0
            while i < 3:
                self.VMs.setdefault(i, []).append(VM(self,0,start_up_delay,i,4,8192, 0.10,False,len(self.VMs[i])))
                i += 1
            j += 1
        #print self.VMs
        # self.worker_indices = range(TOTAL_WORKERS)
        # self.task_distribution = task_distribution

    def add_task_completion_time(self, task_id, completion_time, isLambda):
        task_complete = \
                self.tasks[task_id].task_completed(completion_time)
        if(isLambda == 1):
            self.tasks[task_id].lambda_tasks+=1
        else:
            self.tasks[task_id].vm_tasks+=1
        return task_complete

    def send_tasks(self, task, current_time):
        scheduled_tasks = 0
        global VM_PREDICTION
        new_VM = False
        self.tasks[task.id] = task
        self.task_arrival.setdefault(task.task_type, []).append([int(current_time),task.num_tasks])
        is_scheduled = False
        schedule_events = []
        if schedule_type == 1: ########### lambda for scaleup VM with startup latency ##################
            for i in range(3):
                self.VMs[i] = sorted(self.VMs[i], key=lambda VM:VM.num_queued_tasks,reverse=True)
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled=False
                while i < (len(self.VMs[task.task_type])):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        if self.VMs[task.task_type][i].spin_up:
                            break
                   #     print "free slots", self.VMs[task.task_type][i].free_slots,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],
                            task.id)))
                        is_scheduled = True
                        break
                    i+=1
                if not is_scheduled:
                    self.lambdas.setdefault(task.task_type,[]).append(Lambda(self, current_time, 1000, task.task_type))

                    if(len(self.VMs[task.task_type])):
                        if(len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots < (task.num_tasks)):
                            print "Starting new VM", task.task_type, len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots, task.num_tasks
                            self.VMs.setdefault(task.task_type,[]).append(VM(self,current_time,start_up_delay,task.task_type,4,8192,0.10,True,len(self.VMs[task.task_type])))
                            schedule_events.append((current_time+start_up_delay,VMCreateEvent(self,self.VMs[task.task_type][-1],task.task_type)))

                    schedule_events.append((current_time, ScheduleLambdaEvent(self.lambdas[task.task_type][len(self.lambdas[task.task_type])-1], task.id)))
                scheduled_tasks += 1
            #print "task type" ,self.VMs[task.task_type][i].task_type, "number of scheduled tasks", scheduled_tasks

            return schedule_events
        if schedule_type == 5: ########### lambda for scaleup VM with prediction ##################
            VM_PREDICTION = 1
            #print("lambda for scaleup VM with prediction" ,VM_PREDICTION)
            for i in range(3):
                self.VMs[i] = sorted(self.VMs[i], key=lambda VM:VM.num_queued_tasks,reverse=True)
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled=False
                while i < (len(self.VMs[task.task_type])):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        if self.VMs[task.task_type][i].spin_up:
                            break
                   #     print "free slots", self.VMs[task.task_type][i].free_slots,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],
                            task.id)))
                        is_scheduled = True
                        break
                    i+=1
                if not is_scheduled:
                    self.lambdas.setdefault(task.task_type,[]).append(Lambda(self, current_time, 1000, task.task_type))

                    """if(len(self.VMs[task.task_type])):
                        if(len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots < (task.num_tasks)):
                            print "Starting new VM", task.task_type, len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots, task.num_tasks
                            self.VMs.setdefault(task.task_type,[]).append(VM(self,current_time,start_up_delay,task.task_type,4,8192,0.10,True,len(self.VMs[task.task_type])))
                            schedule_events.append((current_time+start_up_delay,VMCreateEvent(self,self.VMs[task.task_type][-1],task.task_type)))"""

                    schedule_events.append((current_time, ScheduleLambdaEvent(self.lambdas[task.task_type][len(self.lambdas[task.task_type])-1], task.id)))
                scheduled_tasks += 1
            return schedule_events
        if schedule_type == 2: ######## schedule all in lambdas ##################
            while scheduled_tasks < task.num_tasks:
                self.lambdas.setdefault(task.task_type,[]).append(Lambda(self, current_time, 1000, task.task_type))
                schedule_events.append((current_time, ScheduleLambdaEvent(self.lambdas[task.task_type][len(self.lambdas[task.task_type])-1], task.id)))
                scheduled_tasks+=1
            return schedule_events

        if(schedule_type == 3):########  VM with startup latency. ###############
            #print "schedule type is 2"
            for i in range(3):
                self.VMs[i] = sorted(self.VMs[i], key=lambda VM:VM.num_queued_tasks,reverse=True)
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled=False
                while i < len(self.VMs[task.task_type]):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        if self.VMs[task.task_type][i].spin_up:
                            #print i," VM spinning"
                            break
                            #current_time= current_time + start_up_delay - (current_time - self.VMs[task.task_type][i].start_time)
                            #print (current_time - self.VMs[task.task_type][i].start_time),current_time
                        #print "queued slots", self.VMs[task.task_type][i].num_queued_tasks,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],task.id)))
                        is_scheduled = True
                        break
                    i+=1
                if not is_scheduled:

                    #schedule_events.append((current_time+100000,VMCreateEvent(self,task.task_type)))
                    if(len(self.VMs[task.task_type]) * self.VMs[task.task_type][0].max_slots < (task.num_tasks)):
                        print "Starting new VM", task.task_type, len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots, task.num_tasks
                        self.VMs.setdefault(task.task_type,[]).append(VM(self,current_time,start_up_delay,task.task_type,4,8192,0.10,True,len(self.VMs[task.task_type])))
                        schedule_events.append((current_time+start_up_delay,VMCreateEvent(self,self.VMs[task.task_type][-1],task.task_type)))
                        vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                        while(vmid == i):
                            vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                        print "queue behind VM", vmid
                        schedule_events.append((current_time,ScheduleVMEvent(self.VMs[task.task_type][vmid],task.id)))
                    else:
                         vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                         while(vmid == i):
                            vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                         #print "queue behind VM", vmid
                         schedule_events.append((current_time,ScheduleVMEvent(self.VMs[task.task_type][vmid],task.id)))
                scheduled_tasks += 1
            #print "task type" ,self.VMs[task.task_type][i].task_type, "number of scheduled tasks", scheduled_tasks

            return schedule_events
        if(schedule_type == 4):########  VM reactive with buffer. ###############
            #print "schedule type is 2"
            for i in range(3):
                self.VMs[i] = sorted(self.VMs[i], key=lambda VM:VM.num_queued_tasks,reverse=True)
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled=False
                while i < len(self.VMs[task.task_type]):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        if self.VMs[task.task_type][i].spin_up:
                            #print i," VM spinning"
                            break
                            #current_time= current_time + start_up_delay - (current_time - self.VMs[task.task_type][i].start_time)
                            #print (current_time - self.VMs[task.task_type][i].start_time),current_time
                        #print "queued slots", self.VMs[task.task_type][i].num_queued_tasks,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],task.id)))
                        is_scheduled = True
                        break
                    i+=1
                if not is_scheduled:

                    #schedule_events.append((current_time+100000,VMCreateEvent(self,task.task_type)))
                    if(len(self.VMs[task.task_type]) * self.VMs[task.task_type][0].max_slots < (task.num_tasks)):
                        num_vms_needed = int(task.num_tasks/self.VMs[task.task_type][0].max_slots)
                        print "befiore",num_vms_needed
                        num_vms_needed = int(num_vms_needed * burst_threshold) - len(self.VMs[task.task_type])
                        print "Starting new VM",num_vms_needed, task.task_type, len(self.VMs[task.task_type])*self.VMs[task.task_type][0].max_slots, task.num_tasks
                        for j in range(num_vms_needed):
                            self.VMs.setdefault(task.task_type,[]).append(VM(self,current_time,start_up_delay,task.task_type,4,8192,0.10,True,len(self.VMs[task.task_type])))
                            schedule_events.append((current_time+start_up_delay,VMCreateEvent(self,self.VMs[task.task_type][-1],task.task_type)))
                        vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                        while(vmid == i):
                            vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                        print "queue behind VM", vmid
                        schedule_events.append((current_time,ScheduleVMEvent(self.VMs[task.task_type][vmid],task.id)))
                    else:
                         vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                         while(vmid == i):
                            vmid = random.randint(0,len(self.VMs[task.task_type])/2)
                         #print "queue behind VM", vmid
                         schedule_events.append((current_time,ScheduleVMEvent(self.VMs[task.task_type][vmid],task.id)))
                scheduled_tasks += 1
            #print "task type" ,self.VMs[task.task_type][i].task_type, "number of scheduled tasks", scheduled_tasks

            return schedule_events
        if(schedule_type == 6):########  VM with predictions scaling (non reactive). ###############
            #print "schedule type is 2"
            VM_PREDICTION = 1
            for i in range(3):
                self.VMs[i] = sorted(self.VMs[i], key=lambda VM:VM.num_queued_tasks,reverse=True)
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled=False
                while i < len(self.VMs[task.task_type]):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        if self.VMs[task.task_type][i].spin_up:
                            #print i," VM spinning"
                            break
                            #current_time= current_time + start_up_delay - (current_time - self.VMs[task.task_type][i].start_time)
                            #print (current_time - self.VMs[task.task_type][i].start_time),current_time
                        #print "queued slots", self.VMs[task.task_type][i].num_queued_tasks,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],task.id)))
                        is_scheduled = True
                        break
                    i+=1
                if not is_scheduled:
                    vmid = random.randint(0,len(self.VMs[task.task_type])-1)
                         #print "queue behind VM", vmid
                    schedule_events.append((current_time,ScheduleVMEvent(self.VMs[task.task_type][vmid],task.id)))
                scheduled_tasks += 1
            #print "task type" ,self.VMs[task.task_type][i].task_type, "number of scheduled tasks", scheduled_tasks

            return schedule_events
        if(schedule_type == 7):########################## ideal scheduler ##############################
            #print "schedule type is 7"
            while scheduled_tasks < task.num_tasks:
                i=0
                is_scheduled= False
                for i in range(len(self.VMs[task.task_type])):
                    if self.VMs[task.task_type][i].num_queued_tasks < self.VMs[task.task_type][i].max_slots:
                        #print "free slots", self.VMs[task.task_type][i].free_slots,"worker id", self.VMs[task.task_type][i].id,"worker type", self.VMs[task.task_type][i].task_type
                        schedule_events.append((current_time,
                        ScheduleVMEvent(self.VMs[task.task_type][i],task.id)))
                        is_scheduled = True
                        break
                if not is_scheduled:
                    #print 'new instance needed'
                    #schedule_events.append((current_time+:,VMCreateEvent(self,task.task_type)))
                    self.VMs.setdefault(task.task_type,[]).append(VM(self,current_time,60000,task.task_type,4,8192,0.10,False,len(self.VMs[task.task_type])))
                    schedule_events.append((float(current_time),ScheduleVMEvent(self.VMs[task.task_type][-1],task.id)))
                scheduled_tasks += 1
                is_scheduled = False
            return schedule_events

    def calculate_cost(self, end_time):
        total_cost = 0
       #/ f = open(VM_stats_path,'w')
        for i in range(3):
            for j in range(len(self.VMs[i])):
                print >> f,i,",", end_time ,",",self.VMs[i][j].start_time,",", self.VMs[i][j].lastIdleTime
                total_cost+=self.VMs[i][j].price * ((end_time - self.VMs[i][j].start_time)/3600000)
        for i in range(3):
            for j in range(len(self.completed_VMs[i])):
                total_cost+=self.completed_VMs[i][j].price * ((self.completed_VMs[i][j].end_time - self.completed_VMs[i][j].start_time)/3600000)

        return total_cost
    def run(self):
        self.tasks_file = open(self.workload_file, 'r')
        line = self.tasks_file.readline()

        # print line

        start_time = float(line.split(',')[0])
        num_tasks = line.split(',')[3]
        task_type = line.split(',')[2]
        #print start_time, num_tasks, task_type

        # new_task = Task(self, line, 1000, start_time, num_tasks, task_type)

        self.event_queue.put((start_time * 1000, TaskArrival(self,
            start_time * 1000, num_tasks, task_type)))
        last_time = 0
        self.event_queue.put(((start_time*1000)+60000, PeriodicTimerEvent(self)))
        self.event_queue.put(((start_time*1000)+60000, VM_Monitor_Event(self)))
        while not self.event_queue.empty():
            (current_time, event) = self.event_queue.get()
            #print current_time, event, self.event_queue.qsize()
            #assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)
        self.tasks_file.close()
        total_VM_cost = self.calculate_cost(current_time)
        cost_file = open(cost_path,'w')
        print ("total VM cost is",total_VM_cost)
        print >> cost_file,"total VM cost is",total_VM_cost
        self.file_prefix = "pdf"
        complete_jobs = [j for j in self.tasks.values()
                if j.completed_tasks == j.num_tasks]
        print ('%s complete jobs' % len(complete_jobs))
        print >> cost_file,'%s complete jobs' % len(complete_jobs)
        response_times = [job.end_time - job.start_time for job in
                complete_jobs if job.start_time > 500]

        print ("Included %s jobs" % len(response_times))
        plot_cdf(response_times, "%s_response_times.data" % self.file_prefix)

        print ('Average response time: ', np.mean(response_times))
        print >> cost_file ,'Average response time: ', np.mean(response_times)

        total_lambda_cost = 0
        for i in range(3):
            print ("type ",i,"lambda tasks", len(self.lambdas[i]))
            print >> cost_file, "type ",i,"lambda tasks", len(self.lambdas[i])
            print ("type ",i,"lamda cost: ",lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000))
            print >> cost_file , "type ",i,"lamda cost: ",lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000)
            total_lambda_cost+=lambda_cost(len(self.lambdas[i]), self.lambdas[i][0].mem/1024, self.lambdas[i][0].exec_time/1000)
        # longest_tasks = [job.longest_task for job in complete_jobs]
        print ("total lambda cost ", total_lambda_cost)
        print >> cost_file, "total lambda cost ", total_lambda_cost
        print ("total cost of deployment ", total_lambda_cost + total_VM_cost)
        print >> cost_file, "total cost of deployment ", total_lambda_cost + total_VM_cost
        # plot_cdf(longest_tasks, "%s_ideal_response_time.data" % self.file_prefix)

random.seed(1)
os.system("rm -rf %s"%(sys.argv[5]))
os.mkdir(sys.argv[5])
finished_file_path = os.path.join(sys.argv[5], 'finished_file.csv')
all_tasks_path = os.path.join(sys.argv[5], 'all_tasks.csv')
VM_stats_path = os.path.join(sys.argv[5], 'VM_stats.csv')
load_file_path =  os.path.join(sys.argv[5], 'load')
cost_path = os.path.join(sys.argv[5], 'cost')
f = open(VM_stats_path,'w')
logging.basicConfig(level=logging.INFO)
schedule_type = int(sys.argv[2])
load_tracking = int(sys.argv[3])
burst_threshold = float(sys.argv[4])
finished_file = open(finished_file_path,'w')
tasks_file = open(all_tasks_path,'w')
load_file = open(load_file_path,'w')
sim = Simulation(sys.argv[1])
sim.run()
f.close()
load_file.close()


