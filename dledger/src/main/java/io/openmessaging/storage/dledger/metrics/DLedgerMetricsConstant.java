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

public class DLedgerMetricsConstant {

    // metrics name

    public static final String HISTOGRAM_APPEND_ENTRY_LATENCY = "dledger_append_entry_latency";

    public static final String HISTOGRAM_APPEND_ENTRY_BATCH_BYTES = "dledger_append_entry_batch_bytes";

    public static final String HISTOGRAM_APPEND_ENTRY_BATCH_COUNT = "dledger_append_entry_batch_count";

    public static final String HISTOGRAM_REPLICATE_ENTRY_LATENCY = "dledger_replicate_entry_latency";

    public static final String HISTOGRAM_REPLICATE_ENTRY_BATCH_BYTES = "dledger_replicate_entry_batch_bytes";

    public static final String HISTOGRAM_REPLICATE_ENTRY_BATCH_COUNT = "dledger_replicate_entry_batch_count";

    public static final String HISTOGRAM_APPLY_TASK_LATENCY = "dledger_apply_task_latency";

    public static final String HISTOGRAM_APPLY_TASK_BATCH_COUNT = "dledger_apply_task_batch_count";

    public static final String HISTOGRAM_READ_LATENCY = "dledger_read_latency";

    public static final String HISTOGRAM_SAVE_SNAPSHOT_LATENCY = "dledger_save_snapshot_latency";

    public static final String HISTOGRAM_LOAD_SNAPSHOT_LATENCY = "dledger_load_snapshot_latency";

    public static final String HISTOGRAM_INSTALL_SNAPSHOT_LATENCY = "dledger_install_snapshot_latency";

    public static final String GAUGE_ENTRIES_COUNT = "dledger_entries_count";

    public static final String GAUGE_SNAPSHOT_COUNT = "dledger_snapshot_count";

    public static final String GAUGE_ENTRY_STORE_SIZE = "dledger_entry_store_size";


    // label

    public static final String LABEL_GROUP = "group";

    public static final String LABEL_SELF_ID = "self_id";

    public static final String LABEL_REMOTE_ID = "remote_id";

    public static final String LABEL_READ_MODE = "read_mode";

    public static final String METER_NAME = "dledger";

}
