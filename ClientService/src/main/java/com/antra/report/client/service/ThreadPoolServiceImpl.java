package com.antra.report.client.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

@Service
public class ThreadPoolServiceImpl implements ThreadPoolService{

    private final ExecutorService threadPool;

    @Autowired
    public ThreadPoolServiceImpl(@Value("${service.threadpool.corepool}") int core,
                                 @Value("${service.threadpool.maxpool}") int max,
                                 @Value("${service.threadpool.queueSize}") int queueSize) {
        this.threadPool = new ThreadPoolExecutor(
                core, max, 10L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public ExecutorService getThreadPool() {
        return this.threadPool;
    }
}
