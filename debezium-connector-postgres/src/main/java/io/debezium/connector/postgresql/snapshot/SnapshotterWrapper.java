/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;

public class SnapshotterWrapper {

    private final Snapshotter snapshotter;
    private final OffsetState offsetState;
    private final SlotState slotState;
    public SnapshotterWrapper(Snapshotter snapshotter, PostgresConnectorConfig config, OffsetState offsetState, SlotState slotState) {
        this.snapshotter = snapshotter;
        this.offsetState = offsetState;
        this.slotState = slotState;
        this.snapshotter.init(config, offsetState, slotState);
    }

    public Snapshotter getSnapshotter() {
        return this.snapshotter;
    }

    public boolean doesSlotExist() {
        return this.slotState != null;
    }
}
