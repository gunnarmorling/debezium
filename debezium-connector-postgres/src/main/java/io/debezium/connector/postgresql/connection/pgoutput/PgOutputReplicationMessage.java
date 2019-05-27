package io.debezium.connector.postgresql.connection.pgoutput;

import java.time.Instant;
import java.util.List;

import io.debezium.connector.postgresql.connection.ReplicationMessage;

public class PgOutputReplicationMessage implements ReplicationMessage {

    private Operation op;
    private Instant commitTimestamp;
    private long transactionId;
    private String table;
    private List<Column> oldColumns;
    private List<Column> newColumns;

    public PgOutputReplicationMessage(Operation op, String table, Instant commitTimestamp, long transactionId, List<Column> oldColumns, List<Column> newColumns) {
        this.op = op;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public boolean hasTypeMetadata() {
        return true;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }
}
