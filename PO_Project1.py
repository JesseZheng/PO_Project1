# Performance Optimization Project 1
import random
import numpy as np
import csv
import sys, time

class DownloadRequest:

    total_number = 0

    def __init__(self, arrival_date, service_start_date, service_time):
        self.number = DownloadRequest.total_number + 1
        self.arrival_date = arrival_date
        self.service_start_date = service_start_date
        self.service_time = service_time
        self.wait_time = self.service_start_date - self.arrival_date
        self.service_end_date = self.service_start_date + self.service_time
        DownloadRequest.total_number += 1

    def __repr__(self):
        return "Number: %d, arrival_date: %s, service_end_date: %s" \
               % (self.number, self.arrival_date, self.service_end_date)


class Disk:

    def __init__(self, name, servDemand):
        self.name = name
        self.servDemand = servDemand
        self.request_queue = []
        self.qLength = len(self.request_queue)

    def add(self, downloadRequest):
        self.request_queue.append(downloadRequest)
        self.qLength = len(self.request_queue)

    def pop(self):
        self.request_queue = self.request_queue[1:]
        self.qLength = len(self.request_queue)

class CPU:

    def __init__(self, servDemand):
        self.servDemand = servDemand
        self.request_queue = []
        self.qLength = len(self.request_queue)

    def add(self, downloadRequest):
        self.request_queue.append(downloadRequest)
        self.qLength = len(self.request_queue)

    def pop(self):
        self.request_queue = self.request_queue[1:]
        self.qLength = len(self.request_queue)


# A function to sample from negative exponential
# lambd: arrival rate 𝛌 or service rate µ is taken as argument
def neg_exp(lambd):
    sample = random.expovariate(lambd)
    return sample

# A function to sample from  exponential
# beta: service demand S is taken as argument
def exp(beta):
    sample = np.random.exponential(beta)
    return sample


def main():

    cpu = CPU(0.0394)
    disk1 = Disk("Disk1", 0.0771)
    disk2 = Disk("Disk2", 0.1238)
    disk3 = Disk("Disk3", 0.0804)
    disk4 = Disk("Disk4", 0.235)

    cpu_utilization = 0.98
    download_time = 100000
    arrival_rate = cpu_utilization/cpu.servDemand
    service_rate = 1/cpu.servDemand

    broker = 1

    current_t = 0

    completed_requests = []

    while current_t < download_time:

        # CPU service center
        if cpu.qLength == 0:
            arrival_date = neg_exp(arrival_rate)
            service_start_date = arrival_date
        else:
            arrival_date += neg_exp(arrival_rate)
            service_start_date = max(arrival_date, cpu.request_queue[-1].service_end_date)
        service_time = exp(cpu.servDemand)
        # Add the arrival request to CPU queue
        cpu.add(DownloadRequest(arrival_date, service_start_date, service_time))

        # Move served download request from CPU to Disk
        while cpu.request_queue[0].service_end_date <= current_t:
            # Broker process
            if broker == 1:
                if disk1.qLength == 0:
                    print('disk process')

            completed_requests.append(cpu.request_queue[0])
            cpu.pop()



        # Update the time
        current_t = arrival_date
        time.sleep(0.02)
        sys.stdout.write('\r%.2fs: %d download requests completed.' % (current_t, len(completed_requests)))
        sys.stdout.write('\rCPU queue length: %d %s' % (cpu.qLength, cpu.qLength * '|'))
        sys.stdout.flush()

    # calculate summary statistics
    Waits = [a.wait_time for a in completed_requests]
    Mean_Wait = sum(Waits) / len(Waits)

    Total_Times = [a.wait_time + a.service_time for a in completed_requests]
    Mean_Time = sum(Total_Times) / len(Total_Times)

    Service_Times = [a.service_time for a in completed_requests]
    Mean_Service_Time = sum(Service_Times) / len(Service_Times)

    Utilization = sum(Service_Times) / current_t

    # output summary statistics to screen
    print(" ")
    print("Summary results:")
    print("")
    print("Number of customers: ", len(completed_requests))
    print("Mean Service Time: ", Mean_Service_Time)
    print("Mean Wait: ", Mean_Wait)
    print("Mean Time in System: ", Mean_Time)
    print("Utilization: ", Utilization)
    print("")

    # prompt user to output full data set to csv
    if input("Output data to csv (True/False)? ").lower().strip() == "true":
        outfile = open('MM1Q-output-(%s,%s%%,%s).csv' % (arrival_rate, cpu_utilization, download_time), 'w', encoding='utf8', newline='')
        output = csv.writer(outfile)
        output.writerow(['Customer', "Arrival_Date", "Wait", "Service_Start_Date", "Service_Time", "Service_End_Date"])
        for request in completed_requests:
            outrow = []
            outrow.append(request.number)
            outrow.append(request.arrival_date)
            outrow.append(request.wait)
            outrow.append(request.service_start_date)
            outrow.append(request.service_time)
            outrow.append(request.service_end_date)
            output.writerow(outrow)
        outfile.close()
    print("")


if __name__ == "__main__":
    main()