#!/usr/bin/env python

import os, sys
from multiprocessing import Pool
import optparse
import time
import random
import threading
from time import sleep

class ThreadPool(object):
    """Flexible thread pool class.  Creates a pool of threads, then
    accepts tasks that will be dispatched to the next available
    thread."""

    def __init__(self, numThreads):
        """Initialize the thread pool with numThreads workers."""
        self.__threads = []
        self.__resizeLock = threading.Condition(threading.Lock())
        self.__taskLock = threading.Condition(threading.Lock())
        self.__tasks = []
        self.__isJoining = False
        self.setThreadCount(numThreads)

    def setThreadCount(self, newNumThreads):
        """ External method to set the current pool size.  Acquires
        the resizing lock, then calls the internal version to do real
        work."""
        # Can't change the thread count if we're shutting down the pool!
        if self.__isJoining:
            return False

        self.__resizeLock.acquire()
        try:
            self.__setThreadCountNolock(newNumThreads)
        finally:
            self.__resizeLock.release()
        return True

    def __setThreadCountNolock(self, newNumThreads):
        """Set the current pool size, spawning or terminating threads
        if necessary.  Internal use only; assumes the resizing lock is
        held."""
        # If we need to grow the pool, do so
        while newNumThreads > len(self.__threads):
            newThread = ThreadPoolThread(self)
            self.__threads.append(newThread)
            newThread.start()
        # If we need to shrink the pool, do so
        while newNumThreads < len(self.__threads):
            self.__threads[0].goAway()
            del self.__threads[0]

    def getThreadCount(self):
        """Return the number of threads in the pool."""
        self.__resizeLock.acquire()
        try:
            return len(self.__threads)
        finally:
            self.__resizeLock.release()

    def queueTask(self, task, args=None, taskCallback=None):
        """Insert a task into the queue.  task must be callable;
        args and taskCallback can be None."""
        if self.__isJoining == True:
            return False
        if not callable(task):
            return False

        self.__taskLock.acquire()
        try:
            self.__tasks.append((task, args, taskCallback))
            return True
        finally:
            self.__taskLock.release()

    def getNextTask(self):
        """ Retrieve the next task from the task queue.  For use
        only by ThreadPoolThread objects contained in the pool."""
        self.__taskLock.acquire()
        try:
            if self.__tasks == []:
                return (None, None, None)
            else:
                return self.__tasks.pop(0)
        finally:
            self.__taskLock.release()

    def joinAll(self, waitForTasks = True, waitForThreads = True):
        """ Clear the task queue and terminate all pooled threads,
        optionally allowing the tasks and threads to finish."""
        # Mark the pool as joining to prevent any more task queueing
        self.__isJoining = True

        # Wait for tasks to finish
        if waitForTasks:
            while self.__tasks != []:
                sleep(.1)

        # Tell all the threads to quit
        self.__resizeLock.acquire()
        try:
            self.__setThreadCountNolock(0)
            self.__isJoining = True

            # Wait until all threads have exited
            if waitForThreads:
                for t in self.__threads:
                    t.join()
                    del t

            # Reset the pool for potential reuse
            self.__isJoining = False
        finally:
            self.__resizeLock.release()


class ThreadPoolThread(threading.Thread):
    """ Pooled thread class. """
    threadSleepTime = 0.1

    def __init__(self, pool):
        """ Initialize the thread and remember the pool. """
        threading.Thread.__init__(self)
        self.__pool = pool
        self.__isDying = False

    def run(self):
        """ Until told to quit, retrieve the next task and execute
        it, calling the callback if any.  """
        while self.__isDying == False:
            cmd, args, callback = self.__pool.getNextTask()
            # If there's nothing to do, just sleep a bit
            if cmd is None:
                sleep(ThreadPoolThread.threadSleepTime)
            elif callback is None:
                cmd(args)
            else:
                callback(cmd(args))

    def goAway(self):
        """ Exit the run loop next time through."""
        self.__isDying = True

class TestBase(object):
    """Base class for all other tests."""
    def __init__():
        pass
    def __prepare__():
        pass

class FileReadTest(TestBase):
    """Perform simple file open tests."""
    def __init__(self):
        self.path = "/tmp/test"

    def __call__(self, path):
        os.chdir(path)
        dirList=os.listdir(path)
        for f in dirList:
            x = open(f)
            stat_info=os.stat(f)
            x.close()
        sleep(0.01)

def print_timing(func):
    """Decorator to time the execution of a function."""
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
        return res
    return wrapper

def main():
    parser = optparse.OptionParser()
    parser.add_option("-t", "--threads", dest="threads", 
                      help="Number of thread to create", 
                      action="store", metavar="NR", type="int")
    parser.add_option("-n", "--tasks", dest="number_tasks", 
                      help="Number of tasks to execute per process.",
                      action="store", metavar="NR", type="int")
    tests = ["FileReadTest"]
    parser.add_option("--test", dest="test", help="Name of Test to execute",
                      action="store", metavar="TEST", choices=tests, 
                      default="FileReadTest")
    loglevels = ["QUIET","WARN","DEBUG"]
    parser.add_option("-l", "--loglevel", dest="loglevel", 
                      help="Level of logging.", action="store",
                      choices=loglevels)

    group = optparse.OptionGroup(parser, "FileReadTest",
                      "Option to use with the FileReadTest.")
    group.add_option("-p", "--path", action="store", help="Path to directory",
                     dest="path", metavar="PATH", default="/tmp/test")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()
    if not options.threads or not options.number_tasks:
        parser.error("You need to define the number or processes and the number \
                     of tasks.")
    if not options.test:
        parser.error("No test specified. Use --test to specify a test.")

    todays_test = eval(options.test)()
    #todays_test.prepare()

    pool = ThreadPool(options.threads)
    for i in range(options.number_tasks):
        pool.queueTask(todays_test, options.path)
    pool.joinAll()

if __name__ == '__main__':
    main()
