# coding:utf-8
import sys, os
import Queue
import threading


# 替我们工作的线程池中的线程
class MyThread(threading.Thread):
    def __init__(self, workQueue, resultQueue, timeout=30, **kwargs):
        threading.Thread.__init__(self, kwargs=kwargs)
        # 线程在结束前等待任务队列多长时间
        self.timeout = timeout
        self.setDaemon(True)
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.start()

    def run(self):
        while True:
            try:
                # 从工作队列中获取一个任务
                callable, args, kwds = self.workQueue.get(timeout=self.timeout)
                res = callable(*args, **kwds)# 我们要执行的任务
                self.resultQueue.put(res)    # 报任务返回的结果放在结果队列中
            except Queue.Empty:  # 任务队列空的时候结束此线程
                break
            except :
                print sys.exc_info()
                raise

    
class ThreadPool:
    def __init__(self, num_of_threads=10):
        self.workQueue = Queue.Queue()
        self.resultQueue = Queue.Queue()
        self.threads = []
        self.__createThreadPool(num_of_threads)
    
    def __createThreadPool(self, num_of_threads):
        for i in range(num_of_threads):
            thread = MyThread(self.workQueue, self.resultQueue)
            self.threads.append(thread)
    
    def wait_for_complete(self):
        # 等待所有线程完成。
        while len(self.threads):
            thread = self.threads.pop()
            # 等待线程结束
            if thread.isAlive():  # 判断线程是否还存活来决定是否调用join
                thread.join()
        
    def add_job(self, callable, *args, **kwargs):
        self.workQueue.put((callable, args, kwargs))