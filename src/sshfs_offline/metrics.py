
import copy
import logging
import threading
import time
from sshfs_offline import common
from sshfs_offline.log import metricsLogger

class Metrics:
    def __init__(self):
       self.runningThreadCounts: dict[str,int] = dict()
       self.counts: dict[str,int] = dict()
       self.prevCounts: dict[str, int] = dict()
       self.stopped = False

    def start(self):
        """
        Starts a new thread to run the captureLoop method in the background.
        This method initiates the metrics capturing process by launching
        captureLoop in a separate thread, allowing it to run concurrently
        without blocking the main execution flow.
        """
        if common.debug:
            threading.Thread(target=self.captureLoop, daemon=True).start()
            
    def startExecution(self, name: str):
        """
        Marks the start of a thread by incrementing its count in the runningThreadCounts dictionary.
        If the thread name already exists in the dictionary, its count is incremented by 1.
        If it does not exist, it is added to the dictionary with an initial count of 1.
        Args:
            name (str): The name of the thread that is starting.
        """
        if name in self.runningThreadCounts:
            self.runningThreadCounts[name] += 1
        else:
            self.runningThreadCounts[name] = 1

    def endExecution(self, name: str):
        """
        Marks the stop of a thread by decrementing its count in the runningThreadCounts dictionary.        
        If the count reaches 0, the thread name is removed from the dictionary.
        Args:
            name (str): The name of the thread that is stopping.
        """        
        self.runningThreadCounts[name] -= 1
        if self.runningThreadCounts[name] == 0:
            del self.runningThreadCounts[name]

    def incr(self, name: str, count: int=1):
        """
        Increment the count for the given metric name.
        If the metric name already exists in the counts dictionary, its value is incremented by 1.
        If the metric name does not exist, it is added to the dictionary with an initial value of 1.
        Args:
            name (str): The name of the metric to increment.
        """
        if not common.debug:
            return 
        if name in self.counts:
            self.counts[name] += count
        else:
            self.counts[name] = count

    def _logCounts(self):
        """
        Logs the difference in counts for each key between the current and previous state.
        Iterates through sorted keys in `self.counts`, calculates the difference from `self.prevCounts`,
        and logs any positive differences. Updates `self.prevCounts` to the current counts after logging.
        Returns:
            None
        """
        if not common.debug:
            return
        lines: list[str] = []
        diff = 0
        keys = list(self.counts.keys())
        keys.sort()
        for key in keys:
            if key in self.prevCounts: 
                diff = self.counts[key] - self.prevCounts[key]
            else:
                diff = self.counts[key]
            if diff > 0:
                lines.append('\n   {}: {}'.format(key.ljust(32), diff))
        
        self.prevCounts = copy.deepcopy(self.counts)

        runningThreadCountsCopy = copy.deepcopy(self.runningThreadCounts)
        keys = list(runningThreadCountsCopy.keys())
        keys.sort()
        for key in keys:
            lines.append('\n   {}: {}'.format((key+'-running').ljust(32), runningThreadCountsCopy[key]))

        if len(lines) > 0:
            metricsLogger.info(''.join(lines))

    def captureLoop(self):
        """
        Continuously captures and logs metric counts at regular intervals until stopped.
        This method runs an infinite loop, sleeping for 10 seconds between iterations.
        On each iteration, it checks if the `stopped` attribute is set; if so, the loop breaks.
        Otherwise, it calls the `_logCounts` method to log current metrics.
        Any exceptions encountered during execution are logged as errors.
        Raises:
            Logs any exceptions encountered during the loop execution.
        """
        try:  
            common.threadLocal.path = None
            while True:                
                time.sleep(10)
                if self.stopped:
                    break
                self._logCounts()                
        except Exception as e:
            metricsLogger.error('Exception: %s', e)

    def stop(self):
        """
        Stops the metrics collection process.
        Sets the `stopped` flag to True and logs the stop event.
        """
        metricsLogger.info('metrics_stop')
        self.stopped = True

counts: Metrics


            


       