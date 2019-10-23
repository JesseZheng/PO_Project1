# Performance Optimization Project 1
import random
import numpy as np
import csv
import sys, time


# Class for file download request
class DownloadRequest:
    total_number = 0

    def __init__(self, arrival_date, cpu_ssdate, cpu_stime):

        self.number = DownloadRequest.total_number + 1
        self.arrival_date = arrival_date
        self.cpu_service_start_date = cpu_ssdate
        self.disk_service_start_date = 0
        self.cpu_service_time = cpu_stime
        self.disk_service_time = 0

        self.disk_index = -1

        DownloadRequest.total_number += 1

    def __repr__(self):
        return "Number: %d, arrival_date: %s, service_end_date: %s" \
               % (self.number, self.arrival_date, self.get_disk_service_end_date())

    def set_disk_service_start_date(self, disk_ssdate):
        self.disk_service_start_date = disk_ssdate

    def set_disk_service_time(self, disk_service_time):
        self.disk_service_time = disk_service_time

    def set_disk_index(self, disk_index):
        self.disk_index = disk_index

    def get_cpu_service_end_date(self):
        return self.cpu_service_start_date + self.cpu_service_time

    def get_disk_service_end_date(self):
        return self.disk_service_start_date + self.disk_service_time

    def get_total_service_time(self):
        return self.cpu_service_time + self.disk_service_time

    def get_cpu_wait_time(self):
        cpu_wait_time = self.cpu_service_start_date - self.arrival_date
        return cpu_wait_time

    def get_disk_wait_time(self):
        disk_wait_time = self.disk_service_start_date - self.get_cpu_service_end_date()
        return disk_wait_time

    def get_total_wait_time(self):
        return self.get_cpu_wait_time() + self.get_disk_wait_time()

    def get_cpu_response_time(self):
        return self.get_cpu_wait_time() + self.cpu_service_time

    def get_disk_response_time(self):
        return self.get_disk_wait_time() + self.disk_service_time

    def get_total_response_time(self):
        return self.get_total_service_time() + self.get_total_wait_time()


# Class for Service Center(e.g. CPU, disk, etc.)
class ServiceCenter:

    def __init__(self, name, service_demand):
        self.name = name
        self.service_demand = service_demand
        self.request_queue = []
        self.queue_length = len(self.request_queue)

    def __repr__(self):
        return "Name: %s, Service demand: %s, queue length: %d" \
               % (self.name, self.service_demand, self.queue_length)

    def add(self, download_request):
        self.request_queue.append(download_request)
        self.queue_length = len(self.request_queue)

    def pop(self):
        self.request_queue = self.request_queue[1:]
        self.queue_length = len(self.request_queue)

    def get_max_response_time(self):
        if self.queue_length == 0:
            return 0
        else:
            return self.request_queue[-1].get_total_response_time()

    def get_cpu_response_time(self):
        if self.queue_length == 0:
            return 0
        else:
            return self.request_queue[-1].get_cpu_wait_time()


# A function to sample from negative exponential
# lambd: arrival rate 𝛌 or service rate µ is taken as argument
def neg_exp(lambd):
    sample = random.expovariate(lambd)
    return sample


# A function to sample from exponential
# beta: service demand S is taken as argument
def exp(beta):
    sample = np.random.exponential(beta)
    return sample


# Get index of disk that has the earliest service end date of the last download request in 4 disks
def minimal_start_date_broker(disk_group):
    minimal_service_start_date = float('inf')
    rtn_index = -1
    for i in range(4):
        if disk_group[i].queue_length == 0:
            rtn_index = i
            break
        else:
            service_start_date = disk_group[i].request_queue[-1].get_disk_service_end_date()
            if service_start_date < minimal_service_start_date:
                minimal_service_start_date = service_start_date
                rtn_index = i
    if rtn_index == -1:
        rtn_index = 0
    return rtn_index

# Question 4
# Get index of disk that has the shortest queue length
def queue_broker(disk_group):
    minimal_queue_length = float('inf')
    rtn_index = -1
    for i in range(4):
        queue_length = disk_group[i].queue_length
        if queue_length < minimal_queue_length:
            minimal_queue_length = queue_length
            rtn_index = i
    return rtn_index


# Question 2
# in order {disk1, disk2, disk3, disk4}
def simple_broker(index):
    rtn_index = index
    if rtn_index == -1:
        rtn_index = 0
    elif rtn_index == 0:
        rtn_index = 1
    elif rtn_index == 1:
        rtn_index = 2
    elif rtn_index == 2:
        rtn_index = 3
    elif rtn_index == 3:
        rtn_index = 0
    return rtn_index


# Question 3,4,5
# Load Balance broker
def balance_broker(disk_group):
    # the disk
    rtn_index = -1
    balance = float('inf')
    for i in range(len(disk_group)):
        disk = disk_group[i]
        d_balance = disk.queue_length * disk.service_demand
        if d_balance < balance:
            balance = d_balance
            rtn_index = i
    return rtn_index


def MM1_Model():

    cpu = ServiceCenter("CPU", 0.0394)
    disk1 = ServiceCenter("Disk1", 0.0771)
    disk2 = ServiceCenter("Disk2", 0.1238)
    disk3 = ServiceCenter("Disk3", 0.0804)
    disk4 = ServiceCenter("Disk4", 0.235)
    disk_group = [disk1, disk2, disk3, disk4]

    cpu_utilization = 1.2
    max_download_time = 20
    arrival_rate = cpu_utilization/cpu.service_demand
    cpu_theoretic_service_rate = 1/cpu.service_demand

    # Broker strategy
    broker = 1
    disk_index = -1

    # Current date
    current_date = 0
    # max_response_time
    max_response_time = 0
    # the arrival date of present download request
    arrival_date = 0

    # for current service rate
    service_rate = 0
    interval_completed_requests = []
    counter = 0
    total_interval = 0

    cpu_completed_requests = []
    disk_completed_requests = []

    # while current_date <= 20:
    # while current_service_rate < cpu_theoretic_service_rate:
    # while True:
    while max_response_time <= max_download_time:

        # CPU service center
        # ---------------------------------------------------
        if cpu.queue_length == 0:
            # Sample the inter-arrival times
            interval_time = neg_exp(arrival_rate)
            # update the arrival date and service start date
            arrival_date += interval_time
            cpu_service_start_date = arrival_date
        else:
            # Sample the inter-arrival times
            interval_time = neg_exp(arrival_rate)
            # update the arrival date
            arrival_date += interval_time
            # get the service start date
            cpu_service_start_date = max(arrival_date, cpu.request_queue[-1].get_cpu_service_end_date())

        # Sample the service time of this download request in CPU
        # cpu_service_time = exp(cpu.service_demand)
        cpu_service_time = cpu.service_demand
        # Add download request to CPU queue
        cpu.add(DownloadRequest(arrival_date, cpu_service_start_date, cpu_service_time))
        max_response_time = max(max([a.get_cpu_response_time() for a in cpu.request_queue]), max_response_time)
        # ---------------------------------------------------

        # Broker process
        # ---------------------------------------------------
        # Forward the download request completed by CPU to Disk
        if cpu.queue_length > 0:
            cpu_handling_request = cpu.request_queue[0]
            cpu_request_end_date = cpu_handling_request.get_cpu_service_end_date()
            while cpu_request_end_date <= current_date:
                cpu_completed_requests.append(cpu_handling_request)
                # Broker working
                # ---------------------------------------------------
                if broker == 1:
                    disk_index = minimal_start_date_broker(disk_group)
                elif broker == 2:
                    disk_index = queue_broker(disk_group)
                elif broker == 3:
                    disk_index = simple_broker(disk_index)
                elif broker == 4:
                    disk_index = balance_broker(disk_group)
                # ---------------------------------------------------
                cpu_handling_request.set_disk_index(disk_index)
                if disk_group[disk_index].queue_length == 0:
                    disk_service_start_date = cpu_request_end_date
                else:
                    disk_service_start_date = max(cpu_request_end_date,
                                disk_group[disk_index].request_queue[-1].get_disk_service_end_date())
                # Sample the service time of disk
                disk_service_time = exp(disk_group[disk_index].service_demand)
                # Set the disk request variable and then add to disk queue
                cpu_handling_request.set_disk_service_start_date(disk_service_start_date)
                cpu_handling_request.set_disk_service_time(disk_service_time)
                disk_group[disk_index].add(cpu_handling_request)
                cpu.pop()
                cpu_handling_request = cpu.request_queue[0]
                cpu_request_end_date = cpu_handling_request.get_cpu_service_end_date()
        # ---------------------------------------------------

        # Disk service center
        # ---------------------------------------------------
        for disk in disk_group:
            if disk.queue_length > 0:
                disk_handling_request = disk.request_queue[0]
                disk_request_end_date = disk_handling_request.get_disk_service_end_date()
                while disk_request_end_date <= current_date:
                    disk_completed_requests.append(disk_handling_request)
                    # Interval
                    interval_completed_requests.append(disk_handling_request)
                    disk.pop()
                    if disk.queue_length > 0:
                        disk_handling_request = disk.request_queue[0]
                        disk_request_end_date = disk_handling_request.get_disk_service_end_date()
                        # Update the max download time
                        max_response_time = max(max([a.get_total_response_time() for a in disk.request_queue]), max_response_time)
                    else:
                        break
        # ---------------------------------------------------

        # Update current date
        current_date = arrival_date

        # calculate the average service rate(throughput)
        service_rate = len(disk_completed_requests)/current_date

        # CPU and disk Statistics
        cpu_stats = {}
        disk_group_stats = []

        # calculate cpu and disk statistics
        if len(cpu_completed_requests) > 0:
            Waits = [a.get_cpu_wait_time() for a in cpu_completed_requests]
            Mean_Wait = round((sum(Waits) / len(Waits)), 3)

            Service_Times = [a.cpu_service_time for a in cpu_completed_requests]
            Mean_Service_Time = round((sum(Service_Times)/len(Service_Times)), 4)

            Utilization = round((sum(Service_Times)/current_date), 3)

            cpu_stats = {'W': Mean_Wait, 'S': Mean_Service_Time, 'U': Utilization}

        if len(disk_completed_requests) > 0:
            for i in range(4):
                selected_disk = [a for a in disk_completed_requests if a.disk_index == i]
                if len(selected_disk) > 0:
                    Waits = [a.get_disk_wait_time() for a in selected_disk]
                    Mean_Wait = round((sum(Waits) / len(Waits)), 3)

                    Service_Times = [a.disk_service_time for a in selected_disk]
                    Mean_Service_Time = round((sum(Service_Times)/len(Service_Times)), 3)

                    Utilization = round((sum(Service_Times)/current_date), 3)

                    disk_stats = {'W': Mean_Wait, 'S': Mean_Service_Time, 'U': Utilization}
                    disk_group_stats.append(disk_stats)
                else:
                    disk_stats = {'W': None, 'S': None, 'U': None}
                    disk_group_stats.append(disk_stats)

        # Question1 steady state condition
        # if counter < 100:
        #     total_interval += interval_time
        #     counter += 1
        # else:
        #     current_service_rate = len(interval_completed_requests)/total_interval
        #     sys.stdout.write('\rConcurrent download: %s %s - %s %s System service rate: %.2ftps. Number: %s. Interval: %ss. '
        #                      % (cpu.queue_length, [d.queue_length for d in disk_group], counter, arrival_rate, current_service_rate, len(interval_completed_requests), total_interval))
        #     sys.stdout.flush()
        #     interval_completed_requests = []
        #     total_interval = 0
        #     counter = 0

        # Print the real-time information of the system
        sys.stdout.write('\rRun time: %.2fs. Throughput: %.2f. CPU: %s. Disk group: %s. Max download time: %.2fs. Length of queue: %s %s'
                         % (current_date, service_rate, cpu_stats, disk_group_stats, max_response_time, cpu.queue_length, [d.queue_length for d in disk_group]))
        sys.stdout.flush()

        time.sleep(0.02)


    # calculate summary statistics
    Waits = [a.get_total_wait_time() for a in disk_completed_requests]
    Mean_Wait = round((sum(Waits) / len(Waits)), 3)

    Service_Times = [a.get_total_service_time() for a in disk_completed_requests]
    Mean_Service_Time = round(sum(Service_Times) / len(Service_Times), 3)

    Response_Times = [a.get_total_wait_time() + a.get_total_service_time() for a in disk_completed_requests]
    Mean_Response_Time = round((sum(Response_Times) / len(Response_Times)), 3)

    Concurrent_Downloads = sum([d.queue_length for d in disk_group]) + cpu.queue_length

    Utilization = sum(Service_Times) / (current_date*5)

    # output summary statistics to screen
    print(" ")
    print(" ")
    print("Summary results:")
    print("")
    print("Run time: %.2f" % current_date)
    print("Number of downloads completed: ", len(disk_completed_requests))
    # print("Number of concurrent downloads: ", Concurrent_Downloads)
    print("Mean Service Time: ", Mean_Service_Time)
    print("Mean Wait Time: ", Mean_Wait)
    print("Mean Response Time: ", Mean_Response_Time)
    print("System Utilization: %.2f%%" % (Utilization*100))
    print("")

    # prompt user to output full data set to csv
    if input("Output data to csv (True/False)? ").lower().strip() == "true":
        outfile = open('MM1Q-output-(%s,%s%%,%s).csv' % (arrival_rate, cpu_utilization, current_date), 'w', encoding='utf8', newline='')
        output = csv.writer(outfile)
        output.writerow(['Download', "Arrival_Date", "cpu_wait", "cpu_Service_Start_Date", "cpu_Service_Time", "cpu_Service_End_Date",
                         "disk_Service_Start_Date", "disk_Service_Time", "disk_Service_End_Date"])
        for request in disk_completed_requests:
            outrow = []
            outrow.append(request.number)
            outrow.append(request.arrival_date)
            outrow.append(request.get_cpu_wait_time())
            outrow.append(request.cpu_service_start_date)
            outrow.append(request.cpu_service_time)
            outrow.append(request.get_cpu_service_end_date())
            outrow.append(request.get_cpu_service_end_date())
            outrow.append(request.disk_service_time)
            outrow.append(request.get_disk_service_end_date())
            output.writerow(outrow)
        outfile.close()
    print("")


if __name__ == "__main__":
    MM1_Model()

