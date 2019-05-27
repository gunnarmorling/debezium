/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import io.debezium.connector.postgresql.RecordsStreamProducer.PgConnectionSupplier;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;

/**
 * Decodes messages from the PG logical replication plug-in ("pgoutput").
 * See https://www.postgresql.org/docs/10/protocol-logicalrep-message-formats.html for the protocol specification.
 *
 * @author Gunnar Morling
 *
 */
public class PgOutputMessageDecoder implements MessageDecoder {

    private static final Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
    private int relationId;
    private String namespace;
    private String relationName;
    private Instant commitTimestamp;
    private int transactionId;
    List<ColumnMetaData> columns;

    private enum MessageType {
        RELATION,
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE;

        static MessageType forType(char type) {
            switch (type) {
                case 'R': return RELATION;
                case 'B': return BEGIN;
                case 'C': return COMMIT;
                case 'I': return INSERT;
                case 'U': return UPDATE;
                case 'D': return DELETE;
                default: throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        MessageType type = MessageType.forType((char) buffer.get());

        if (type == MessageType.INSERT) {
            // TODO move to enum: type.handle(buffer, processor);
            System.out.println("### Received INSERT event");
            int relationId = buffer.getInt();
            char tupelDataType = (char) buffer.get();
            short numberOfColumns = buffer.getShort();
            List<Column> columns = new ArrayList<>(numberOfColumns);
            for(int i = 0; i < numberOfColumns; i++) {
                char dataType = (char) buffer.get();
                if (dataType == 't') {
                    int valueLength = buffer.getInt();
                    byte[] value = new byte[valueLength];
                    buffer.get(value, 0, valueLength);
                    String valueStr = new String(value);
                    ColumnMetaData metadata = this.columns.get(i);
                    columns.add(new AbstractReplicationMessageColumn(metadata.getColumnName(), metadata.getPostgresType(), metadata.getPostgresType().getName(), true, true) {

                        @Override
                        public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                            return valueStr;
                        }
                    });
                    System.out.println(valueStr);
                }
            }

            processor.process( new PgOutputReplicationMessage(
                    Operation.INSERT,
                    relationName,
                    commitTimestamp,
                    transactionId,
                    null,
                    columns
            ));
        }
        else if (type == MessageType.RELATION) {
            System.out.println("### Received RELATION event");
            relationId = buffer.getInt();
            namespace = readString(buffer);
            relationName = readString(buffer);
            buffer.get(); // replica identity
            short columnCount = buffer.getShort();
            columns = new ArrayList<>(columnCount);

            for(short i = 0; i < columnCount; i++) {
                byte flags = buffer.get();
                boolean partOfPk = flags == 1;
                System.out.println("PK: " + partOfPk);
                String columnName = readString(buffer);
                System.out.println(columnName);
                int columnType = buffer.getInt(); // data type
                columns.add(new ColumnMetaData(columnName, typeRegistry.get(columnType)));
                buffer.getInt(); // TODO atttypmod
            }
        }

        else if (type == MessageType.BEGIN) {
            System.out.println("### Received BEGIN event");
            buffer.getLong(); // LSN
            commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
            transactionId = buffer.getInt();
        }
        if (type == MessageType.COMMIT) {
            System.out.println("### Received COMMIT event");
        }
        else {
            System.out.println("### Received event: " + type);
        }
    }

    private String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0){
            sb.append((char)b);
        }
        return sb.toString();
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return builder.withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", "dbz_publication");
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder tryOnceOptions(ChainedLogicalStreamBuilder builder) {
        return builder;
    }
}
