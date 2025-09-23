
from logging import getLogger
import copy
import logging
import threading
import time
import log


class Metrics:
    def __init__(self):
       self.log = getLogger(log.METRICS)   
       self.counts: dict[str,int] = dict()
       self.prevCounts: dict[str, int] = dict()
       self.stopped = False

    def start(self):
        threading.Thread(target=self.captureLoop).start()
        
    def incr(self, name: str): 
        if name in self.counts:
            self.counts[name] += 1
        else:
            self.counts[name] = 1                   

    def _logCounts(self):
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
                lines.append('\n   {}: {}'.format(key.ljust(16), diff))
        
        self.prevCounts = copy.deepcopy(self.counts)
        
        if len(lines) > 0:
            self.log.info(''.join(lines))

    def captureLoop(self):
        try:            
            while True:                
                time.sleep(10)
                if self.stopped:
                    break
                self._logCounts()                
        except Exception as e:
            self.log.error('Exception: %s', e)

    def stop(self):
        self.log.info('metrics_stop')
        self.stopped = True

counts: Metrics


            


       