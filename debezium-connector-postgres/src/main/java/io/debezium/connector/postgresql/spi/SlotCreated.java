/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import io.debezium.annotation.Incubating;
import org.postgresql.replication.LogSequenceNumber;

/**
 * A simple data container for the result of @{link: }
 */
@Incubating
public class SlotCreated {
    private final String slotName;
    private final Long walStartLsn;
    private final String snapshotName;
    private final String pluginName;

    public SlotCreated(String name, String startLsn, String snapshotName, String pluginName) {
        this.slotName = name;
        this.walStartLsn = LogSequenceNumber.valueOf(startLsn).asLong();
        this.snapshotName = snapshotName;
        this.pluginName = pluginName;
    }


    /**
     * return's the
     * @return
     */
    public String slotName() {
        return slotName;
    }

    public Long startLsn() {
        return walStartLsn;
    }

    public String snapshotName() {
        return snapshotName;
    }

    public String pluginName() {
        return pluginName;
    }
}
