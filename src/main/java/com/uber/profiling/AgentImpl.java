/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.profiling;

import com.uber.profiling.profilers.CpuAndMemoryProfiler;
import com.uber.profiling.profilers.IOProfiler;
import com.uber.profiling.profilers.MethodArgumentCollector;
import com.uber.profiling.profilers.MethodArgumentProfiler;
import com.uber.profiling.profilers.MethodDurationCollector;
import com.uber.profiling.profilers.MethodDurationProfiler;
import com.uber.profiling.profilers.ProcessInfoProfiler;
import com.uber.profiling.profilers.StacktraceCollectorProfiler;
import com.uber.profiling.profilers.StacktraceReporterProfiler;
import com.uber.profiling.transformers.JavaAgentFileTransformer;
import com.uber.profiling.transformers.MethodProfilerStaticProxy;
import com.uber.profiling.util.AgentLogger;
import com.uber.profiling.util.ClassAndMethodLongMetricBuffer;
import com.uber.profiling.util.ClassMethodArgumentMetricBuffer;
import com.uber.profiling.util.SparkUtils;
import com.uber.profiling.util.StacktraceMetricBuffer;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AgentImpl {
    public static final String VERSION = "1.0.0";
    
    private static final AgentLogger logger = AgentLogger.getLogger(AgentImpl.class.getName());

    private static final int MAX_THREAD_POOL_SIZE = 2;

    private boolean started = false;

    private Map<String, Profiler> profilers = new ConcurrentHashMap<>();

    public void run(Arguments arguments, Instrumentation instrumentation, Collection<AutoCloseable> objectsToCloseOnShutdown) {
        if (arguments.isNoop()) {
            logger.info("Agent noop is true, do not run anything");
            return;
        }
        
        Reporter reporter = arguments.getReporter();

        String processUuid = UUID.randomUUID().toString();

        String appId = null;
        
        String appIdVariable = arguments.getAppIdVariable();
        if (appIdVariable != null && !appIdVariable.isEmpty()) {
            appId = System.getenv(appIdVariable);
        }
        
        if (appId == null || appId.isEmpty()) {
            appId = SparkUtils.probeAppId(arguments.getAppIdRegex());
        }

        if (!arguments.getDurationProfiling().isEmpty()
                || !arguments.getArgumentProfiling().isEmpty()) {
            instrumentation.addTransformer(new JavaAgentFileTransformer(arguments.getDurationProfiling(), arguments.getArgumentProfiling()));
        }

        Map<String, Profiler> profilers = createProfilers(reporter, arguments, processUuid, appId);
        
        ProfilerGroup profilerGroup = startProfilers(profilers, arguments);

        if (profilerGroup!=null) {
            Thread shutdownHook = new Thread(
                new ShutdownHookRunner(profilerGroup.getPeriodicProfilers(),
                    Arrays.asList(reporter), objectsToCloseOnShutdown));
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    public ProfilerGroup startProfilers(Map<String, Profiler> profilers, Arguments arguments) {
        if (started) {
            logger.warn("Profilers already started, do not start it again");
            Profiler profiler = profilers.get(StacktraceCollectorProfiler.PROFILER_NAME);

            if (profiler == null){
                Profiler reporterProfiler = profilers.get(StacktraceReporterProfiler.PROFILER_NAME);
                if (reporterProfiler!=null) {
                    profiler = profilers.computeIfAbsent(StacktraceCollectorProfiler.PROFILER_NAME, k -> {
                        StacktraceCollectorProfiler stacktraceCollectorProfiler = new StacktraceCollectorProfiler(
                            ((StacktraceReporterProfiler) reporterProfiler).getBuffer(),
                            AgentThreadFactory.NAME_PREFIX);
                        stacktraceCollectorProfiler.setIntervalMillis(arguments.getSampleInterval());
                        return stacktraceCollectorProfiler;
                    });
                }
            } else if (profiler.getIntervalMillis()!=arguments.getSampleInterval()){
                //cancel old
                profiler.cancel();
                logger.info("Canceled " + profiler + " due to sampleInterval param change");
            }

            if (profiler!=null){
                if (arguments.getSampleInterval() < Arguments.MIN_INTERVAL_MILLIS) {
                    logger.warn("Won't start:" + profiler + "Interval too short, must be at least " + Arguments.MIN_INTERVAL_MILLIS);
                } else {
                    ProfilerRunner worker = new ProfilerRunner(profiler);
                    ((StacktraceCollectorProfiler)profiler).setIntervalMillis(arguments.getSampleInterval());
                    ScheduledFuture<?> handler = scheduledExecutorService.scheduleAtFixedRate(worker, 0, profiler.getIntervalMillis(), TimeUnit.MILLISECONDS);
                    profiler.setHandler(handler);
                }
            }
            return null;
        }

        List<Profiler> oneTimeProfilers = new ArrayList<>();
        List<Profiler> periodicProfilers = new ArrayList<>();

        for (Profiler profiler : profilers.values()) {
            if (profiler.getIntervalMillis() == 0) {
                oneTimeProfilers.add(profiler);
            } else if (profiler.getIntervalMillis() > 0) {
                periodicProfilers.add(profiler);
            } else {
                logger.log(String.format("Ignored profiler %s due to its invalid interval %s", profiler, profiler.getIntervalMillis()));
            }
        }

        for (Profiler profiler : oneTimeProfilers) {
            try {
                profiler.profile();
                logger.info("Finished one time profiler: " + profiler);
            } catch (Throwable ex) {
                logger.warn("Failed to run one time profiler: " + profiler, ex);
            }
        }

        for (Profiler profiler : periodicProfilers) {
            try {
                profiler.profile();
                logger.info("Ran periodic profiler (first run): " + profiler);
            } catch (Throwable ex) {
                logger.warn("Failed to run periodic profiler (first run): " + profiler, ex);
            }
        }
        
        scheduleProfilers(periodicProfilers);
        started = true;

        return new ProfilerGroup(oneTimeProfilers, periodicProfilers);
    }

    private Map<String, Profiler> createProfilers(Reporter reporter, Arguments arguments, String processUuid, String appId) {
        String tag = arguments.getTag();
        String cluster = arguments.getCluster();
        long metricInterval = arguments.getMetricInterval();

        profilers.computeIfAbsent(CpuAndMemoryProfiler.PROFILER_NAME, k -> {
            CpuAndMemoryProfiler cpuAndMemoryProfiler = new CpuAndMemoryProfiler(reporter);
            cpuAndMemoryProfiler.setTag(tag);
            cpuAndMemoryProfiler.setCluster(cluster);
            cpuAndMemoryProfiler.setIntervalMillis(metricInterval);
            cpuAndMemoryProfiler.setProcessUuid(processUuid);
            cpuAndMemoryProfiler.setAppId(appId);
            return cpuAndMemoryProfiler;
        } );

        profilers.computeIfAbsent(ProcessInfoProfiler.PROFILER_NAME, k -> {
            ProcessInfoProfiler processInfoProfiler = new ProcessInfoProfiler(reporter);
            processInfoProfiler.setTag(tag);
            processInfoProfiler.setCluster(cluster);
            processInfoProfiler.setProcessUuid(processUuid);
            processInfoProfiler.setAppId(appId);
            return processInfoProfiler;
        });


        if (!arguments.getDurationProfiling().isEmpty()) {
            profilers.computeIfAbsent(MethodDurationProfiler.PROFILER_NAME, k -> {
                ClassAndMethodLongMetricBuffer classAndMethodMetricBuffer = new ClassAndMethodLongMetricBuffer();

                MethodDurationProfiler methodDurationProfiler = new MethodDurationProfiler(classAndMethodMetricBuffer, reporter);
                methodDurationProfiler.setTag(tag);
                methodDurationProfiler.setCluster(cluster);
                methodDurationProfiler.setIntervalMillis(metricInterval);
                methodDurationProfiler.setProcessUuid(processUuid);
                methodDurationProfiler.setAppId(appId);

                MethodDurationCollector methodDurationCollector = new MethodDurationCollector(classAndMethodMetricBuffer);
                MethodProfilerStaticProxy.setCollector(methodDurationCollector);
                return methodDurationProfiler;
            });
        }

        if (!arguments.getArgumentProfiling().isEmpty()) {
            profilers.computeIfAbsent(MethodArgumentCollector.PROFILER_NAME, k -> {
                    ClassMethodArgumentMetricBuffer classAndMethodArgumentBuffer = new ClassMethodArgumentMetricBuffer();

                    MethodArgumentProfiler methodArgumentProfiler = new MethodArgumentProfiler(
                        classAndMethodArgumentBuffer, reporter);
                    methodArgumentProfiler.setTag(tag);
                    methodArgumentProfiler.setCluster(cluster);
                    methodArgumentProfiler.setIntervalMillis(metricInterval);
                    methodArgumentProfiler.setProcessUuid(processUuid);
                    methodArgumentProfiler.setAppId(appId);

                    MethodArgumentCollector methodArgumentCollector = new MethodArgumentCollector(
                        classAndMethodArgumentBuffer);
                    MethodProfilerStaticProxy.setArgumentCollector(methodArgumentCollector);
                    return methodArgumentProfiler;
                });

        }

        if (metricInterval > 0) {
            StacktraceMetricBuffer stacktraceMetricBuffer = new StacktraceMetricBuffer();
            profilers
                .computeIfAbsent(StacktraceReporterProfiler.PROFILER_NAME, k -> {
                    StacktraceReporterProfiler stacktraceReporterProfiler = new StacktraceReporterProfiler(
                        stacktraceMetricBuffer, reporter);
                    stacktraceReporterProfiler.setTag(tag);
                    stacktraceReporterProfiler.setCluster(cluster);
                    stacktraceReporterProfiler.setIntervalMillis(metricInterval);
                    stacktraceReporterProfiler.setProcessUuid(processUuid);
                    stacktraceReporterProfiler.setAppId(appId);
                    return stacktraceReporterProfiler;
                });
        }

        if (arguments.getSampleInterval() > 0) {
            Profiler reporterProfiler = profilers.get(StacktraceReporterProfiler.PROFILER_NAME);
            if (reporterProfiler!=null) {
                profilers.computeIfAbsent(StacktraceCollectorProfiler.PROFILER_NAME, k -> {
                    StacktraceCollectorProfiler stacktraceCollectorProfiler = new StacktraceCollectorProfiler(
                        ((StacktraceReporterProfiler) reporterProfiler).getBuffer(),
                        AgentThreadFactory.NAME_PREFIX);
                    stacktraceCollectorProfiler.setIntervalMillis(arguments.getSampleInterval());
                    return stacktraceCollectorProfiler;
                });
            }
        }

        if (arguments.isIoProfiling()) {
            profilers.computeIfAbsent(IOProfiler.PROFILER_NAME, k -> {
                IOProfiler ioProfiler = new IOProfiler(reporter);
                ioProfiler.setTag(tag);
                ioProfiler.setCluster(cluster);
                ioProfiler.setIntervalMillis(metricInterval);
                ioProfiler.setProcessUuid(processUuid);
                ioProfiler.setAppId(appId);

                return ioProfiler;
            });
        }
        
        return profilers;
    }

    private ScheduledExecutorService scheduledExecutorService;
    private void scheduleProfilers(Collection<Profiler> profilers) {
        int threadPoolSize = Math.min(profilers.size(), MAX_THREAD_POOL_SIZE);
        scheduledExecutorService = Executors.newScheduledThreadPool(threadPoolSize, new AgentThreadFactory());

        for (Profiler profiler : profilers) {
            if (profiler.getIntervalMillis() < Arguments.MIN_INTERVAL_MILLIS) {
                throw new RuntimeException("Interval too short for profiler: " + profiler + ", must be at least " + Arguments.MIN_INTERVAL_MILLIS);
            }
            
            ProfilerRunner worker = new ProfilerRunner(profiler);
            ScheduledFuture<?> handler = scheduledExecutorService.scheduleAtFixedRate(worker, 0, profiler.getIntervalMillis(), TimeUnit.MILLISECONDS);
            profiler.setHandler(handler);
            logger.info(String.format("Scheduled profiler %s with interval %s millis", profiler, profiler.getIntervalMillis()));
        }
    }
}
