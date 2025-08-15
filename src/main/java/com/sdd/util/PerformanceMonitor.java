package com.sdd.util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PerformanceMonitor {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);
    private final ScheduledExecutorService scheduler;
    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private volatile boolean monitoring = false;

    public PerformanceMonitor() {
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.threadBean = ManagementFactory.getThreadMXBean();
    }

    public void startMonitoring() {
        if (monitoring) return;

        monitoring = true;
        logger.info("Starting performance monitoring...");

        scheduler.scheduleAtFixedRate(this::logPerformanceMetrics, 0, 30, TimeUnit.SECONDS);
    }

    public void stopMonitoring() {
        if (!monitoring) return;

        monitoring = false;
        scheduler.shutdown();
        logger.info("Performance monitoring stopped");
    }

    private void logPerformanceMetrics() {
        try {
            // Memory metrics
            long heapUsed = memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
            long heapMax = memoryBean.getHeapMemoryUsage().getMax() / (1024 * 1024);
            long nonHeapUsed = memoryBean.getNonHeapMemoryUsage().getUsed() / (1024 * 1024);

            // Thread metrics
            int threadCount = threadBean.getThreadCount();
            int daemonThreadCount = threadBean.getDaemonThreadCount();

            logger.info("PERFORMANCE METRICS - Heap: {}/{} MB, Non-Heap: {} MB, Threads: {} (Daemon: {})",
                    heapUsed, heapMax, nonHeapUsed, threadCount, daemonThreadCount);

            // Warning thresholds
            double heapUsagePercent = (double) heapUsed / heapMax * 100;
            if (heapUsagePercent > 80) {
                logger.warn("HIGH MEMORY USAGE: {}% heap used", String.format("%.1f", heapUsagePercent));
            }

            if (threadCount > 50) {
                logger.warn("HIGH THREAD COUNT: {} threads active", threadCount);
            }

        } catch (Exception e) {
            logger.debug("Error collecting performance metrics: {}", e.getMessage());
        }
    }
}
