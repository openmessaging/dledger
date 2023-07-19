/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.metrics;

import com.google.common.base.Splitter;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.GAUGE_ENTRY_STORE_SIZE;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.GAUGE_LAG_ENTRIES_COUNT;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.GAUGE_SNAPSHOT_COUNT;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_APPEND_ENTRY_BATCH_BYTES;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_APPEND_ENTRY_BATCH_COUNT;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_APPEND_ENTRY_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_APPLY_TASK_BATCH_COUNT;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_APPLY_TASK_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_INSTALL_SNAPSHOT_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_LOAD_SNAPSHOT_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_READ_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_REPLICA_ENTRY_BATCH_BYTES;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_REPLICA_ENTRY_BATCH_COUNT;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_REPLICATE_ENTRY_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.HISTOGRAM_SAVE_SNAPSHOT_LATENCY;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.LABEL_GROUP;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.LABEL_SELF_ID;
import static io.openmessaging.storage.dledger.metrics.DLedgerMetricsConstant.METER_NAME;

public class DLedgerMetricsManager {
    private static final Logger logger = LoggerFactory.getLogger(DLedgerMetricsManager.class);

    private static double us = 1d;

    private static double ms = 1000 * us;

    private static double s = 1000 * ms;
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    public static LongHistogram appendEntryLatency = new NopLongHistogram();

    public static LongHistogram appendEntryBatchBytes = new NopLongHistogram();

    public static LongHistogram appendEntryBatchCount = new NopLongHistogram();

    public static LongHistogram replicateEntryLatency = new NopLongHistogram();

    public static LongHistogram replicaEntryBatchBytes = new NopLongHistogram();

    public static LongHistogram replicaEntryBatchCount = new NopLongHistogram();

    public static LongHistogram applyTaskLatency = new NopLongHistogram();

    public static LongHistogram applyTaskBatchCount = new NopLongHistogram();

    public static LongHistogram readLatency = new NopLongHistogram();

    public static LongHistogram saveSnapshotLatency = new NopLongHistogram();

    public static LongHistogram loadSnapshotLatency = new NopLongHistogram();

    public static LongHistogram installSnapshotLatency = new NopLongHistogram();

    public static ObservableLongGauge lagEntriesCount = new NopObservableLongGauge();

    public static ObservableLongGauge snapshotCount = new NopObservableLongGauge();

    public static ObservableLongGauge entryStoreSize = new NopObservableLongGauge();

    public static OtlpGrpcMetricExporter otlpGrpcMetricExporter;

    public static PeriodicMetricReader periodicMetricReader;

    public static PrometheusHttpServer prometheusHttpServer;

    public static LoggingMetricExporter loggingMetricsExporter;

    public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier, DLedgerServer server) {
        DLedgerMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;

        appendEntryLatency = meter.histogramBuilder(HISTOGRAM_APPEND_ENTRY_LATENCY)
            .setDescription("DLedger append entry latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        appendEntryBatchBytes = meter.histogramBuilder(HISTOGRAM_APPEND_ENTRY_BATCH_BYTES)
            .setDescription("DLedger append entry batch bytes")
            .setUnit("bytes")
            .ofLongs()
            .build();

        appendEntryBatchCount = meter.histogramBuilder(HISTOGRAM_APPEND_ENTRY_BATCH_COUNT)
            .setDescription("DLedger append entry batch count")
            .setUnit("count")
            .ofLongs()
            .build();

        replicateEntryLatency = meter.histogramBuilder(HISTOGRAM_REPLICATE_ENTRY_LATENCY)
            .setDescription("DLedger replica entry latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        replicaEntryBatchBytes = meter.histogramBuilder(HISTOGRAM_REPLICA_ENTRY_BATCH_BYTES)
            .setDescription("DLedger replica entry batch bytes")
            .setUnit("bytes")
            .ofLongs()
            .build();

        replicaEntryBatchCount = meter.histogramBuilder(HISTOGRAM_REPLICA_ENTRY_BATCH_COUNT)
            .setDescription("DLedger replica entry batch count")
            .setUnit("count")
            .ofLongs()
            .build();

        applyTaskLatency = meter.histogramBuilder(HISTOGRAM_APPLY_TASK_LATENCY)
            .setDescription("DLedger apply task latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        applyTaskBatchCount = meter.histogramBuilder(HISTOGRAM_APPLY_TASK_BATCH_COUNT)
            .setDescription("DLedger apply task batch count")
            .setUnit("count")
            .ofLongs()
            .build();

        readLatency = meter.histogramBuilder(HISTOGRAM_READ_LATENCY)
            .setDescription("DLedger read latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        saveSnapshotLatency = meter.histogramBuilder(HISTOGRAM_SAVE_SNAPSHOT_LATENCY)
            .setDescription("DLedger save snapshot latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        loadSnapshotLatency = meter.histogramBuilder(HISTOGRAM_LOAD_SNAPSHOT_LATENCY)
            .setDescription("DLedger load snapshot latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        installSnapshotLatency = meter.histogramBuilder(HISTOGRAM_INSTALL_SNAPSHOT_LATENCY)
            .setDescription("DLedger install snapshot latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        lagEntriesCount = meter.gaugeBuilder(GAUGE_LAG_ENTRIES_COUNT)
            .setDescription("DLedger lag entries count")
            .setUnit("count")
            .ofLongs()
            .buildWithCallback(measurement -> {

            });

        snapshotCount = meter.gaugeBuilder(GAUGE_SNAPSHOT_COUNT)
            .setDescription("DLedger snapshot count")
            .setUnit("count")
            .ofLongs()
            .buildWithCallback(measurement -> {

            });

        entryStoreSize = meter.gaugeBuilder(GAUGE_ENTRY_STORE_SIZE)
            .setDescription("DLedger entry store size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {

            });
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder builder = attributesBuilderSupplier != null ? attributesBuilderSupplier.get() : Attributes.builder();
        return builder;
    }

    private static boolean checkConfig(DLedgerConfig config) {
        if (config == null) {
            return false;
        }
        MetricsExporterType exporterType = config.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(config.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
            case LOG:
                return true;
        }
        return false;
    }

    public static void defaultInit(DLedgerServer server) {
        DLedgerConfig config = server.getdLedgerConfig();
        MetricsExporterType type = config.getMetricsExporterType();
        if (type == MetricsExporterType.DISABLE) {
            return;
        }

        if (!checkConfig(config)) {
            logger.error("Metrics exporter config is invalid, please check it");
            return;
        }

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder().setResource(Resource.empty());

        if (type == MetricsExporterType.OTLP_GRPC) {
            String endpoint = config.getMetricsGrpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(config.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS);

            String headers = config.getMetricsGrpcExporterHeader();
            if (StringUtils.isNotBlank(headers)) {
                Map<String, String> headerMap = new HashMap<>();
                List<String> headerList = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                for (String header : headerList) {
                    String[] pair = header.split(":");
                    if (pair.length != 2) {
                        logger.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(pair[0], pair[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            otlpGrpcMetricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(otlpGrpcMetricExporter)
                .setInterval(config.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        if (type == MetricsExporterType.PROM) {
            String promExporterHost = config.getMetricsPromExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                promExporterHost = "0.0.0.0";
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(config.getMetricsPromExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        }

        if (type == MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricsExporter = LoggingMetricExporter.create(AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(LoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricsExporter)
                .setInterval(config.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        registerMetricsView(providerBuilder);

        Meter meter = OpenTelemetrySdk.builder().setMeterProvider(providerBuilder.build())
            .build().getMeter(METER_NAME);
        AttributesBuilder builder = Attributes.builder().put(LABEL_GROUP, config.getGroup()).put(LABEL_SELF_ID, config.getSelfId());
        init(meter, () -> {
            return Attributes.builder().put(LABEL_GROUP, config.getGroup()).put(LABEL_SELF_ID, config.getSelfId());
        }, server);
    }

    private static void registerMetricsView(SdkMeterProviderBuilder providerBuilder) {
        // define latency bucket
        List<Double> latencyBuckets = Arrays.asList(
            1 * us, 3 * us, 5 * us,
            10 * us, 30 * us, 50 * us,
            100 * us, 300 * us, 500 * us,
            1 * ms, 2 * ms, 3 * ms, 5 * ms,
            10 * ms, 30 * ms, 50 * ms,
            100 * ms, 300 * ms, 500 * ms,
            1 * s, 3 * s, 5 * s,
            10 * s
        );

        View latecyView = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(latencyBuckets))
            .build();

        InstrumentSelector appendEntryLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_APPEND_ENTRY_LATENCY)
            .build();

        InstrumentSelector replicateEntryLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_REPLICATE_ENTRY_LATENCY)
            .build();

        InstrumentSelector applyTaskLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_APPLY_TASK_LATENCY)
            .build();

        InstrumentSelector readLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_READ_LATENCY)
            .build();

        InstrumentSelector saveSnapshotLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_SAVE_SNAPSHOT_LATENCY)
            .build();

        InstrumentSelector loadSnapshotLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_LOAD_SNAPSHOT_LATENCY)
            .build();

        InstrumentSelector installSnapshotLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_INSTALL_SNAPSHOT_LATENCY)
            .build();

        providerBuilder.registerView(appendEntryLatencySelector, latecyView);
        providerBuilder.registerView(replicateEntryLatencySelector, latecyView);
        providerBuilder.registerView(applyTaskLatencySelector, latecyView);
        providerBuilder.registerView(readLatencySelector, latecyView);
        providerBuilder.registerView(saveSnapshotLatencySelector, latecyView);
        providerBuilder.registerView(loadSnapshotLatencySelector, latecyView);
        providerBuilder.registerView(installSnapshotLatencySelector, latecyView);
    }

}

