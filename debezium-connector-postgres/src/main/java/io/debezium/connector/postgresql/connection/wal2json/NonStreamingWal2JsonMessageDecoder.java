/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.document.Array;
import io.debezium.document.Array.Entry;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.Value;
import io.debezium.time.Conversions;

/**
 * A non-streaming version of JSON deserialization of a message sent by
 * <a href="https://github.com/eulerto/wal2json">wal2json</a> logical decoding plugin. The plugin sends all
 * changes in one transaction as a single batch in a big JSON file and they are passed to processor one-by-one.
 *
 * @author Jiri Pechanec
 *
 */

public class NonStreamingWal2JsonMessageDecoder implements MessageDecoder {

    private static final  Logger LOGGER = LoggerFactory.getLogger(NonStreamingWal2JsonMessageDecoder.class);

    private final DateTimeFormat dateTime = DateTimeFormat.get();
    private boolean containsMetadata = false;

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            final Document message = DocumentReader.floatNumbersAsTextReader().read(content);
            LOGGER.debug("Message arrived for decoding {}", message);
            final long txId = message.getLong("xid");
            final String timestamp = message.getString("timestamp");
            final Instant commitTime = Conversions.toInstant(dateTime.systemTimestamp(timestamp));
            final Array changes = message.getArray("change");

            Iterator<Entry> it = changes.iterator();
            while (it.hasNext()) {
                Value value = it.next().getValue();
                processor.process(new Wal2JsonReplicationMessage(txId, commitTime, value.asDocument(), containsMetadata, !it.hasNext(), typeRegistry));
            }
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return optionsWithoutMetadata(builder)
            .withSlotOption("include-not-null", "true");
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder
            .withSlotOption("pretty-print", 1)
            .withSlotOption("write-in-chunks", 0)
            .withSlotOption("include-xids", 1)
            .withSlotOption("include-timestamp", 1);
    }

    @Override
    public ChainedLogicalStreamBuilder tryOnceOptions(ChainedLogicalStreamBuilder builder) {
        return builder.withSlotOption("include-unchanged-toast", 0);
    }

    @Override
    public void setContainsMetadata(boolean containsMetadata) {
        this.containsMetadata = containsMetadata;
    }
}
