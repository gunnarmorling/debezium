/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.junit.ConditionalFail;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Integration test for the {@link RecordsStreamProducer} class. This also tests indirectly the PG plugin functionality for
 * different use cases.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsStreamProducerIT extends AbstractRecordsProducerTest {

    private RecordsStreamProducer recordsProducer;
    private TestConsumer consumer;
    private final Consumer<Throwable> blackHole = t -> {};

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    @Before
    public void before() throws Exception {
        // ensure the slot is deleted for each test
        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_postgis.ddl");
        String statements =
                "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS test_table;" +
                "CREATE TABLE test_table (pk SERIAL, text TEXT, PRIMARY KEY(pk));" +
                "CREATE TABLE table_with_interval (id SERIAL PRIMARY KEY, title VARCHAR(512) NOT NULL, time_limit INTERVAL DEFAULT '60 days'::INTERVAL NOT NULL);" +
                "INSERT INTO test_table(text) VALUES ('insert');";
        TestHelper.execute(statements);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SCHEMA_BLACKLIST, "postgis")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("database.replication", "database")
                .with("database.preferQueryMode", "simple")
                .with("assumeMinServerVersion.set", "9.4")
                .build());
        setupRecordsProducer(config);
    }

    @After
    public void after() throws Exception {
        if (recordsProducer != null) {
            recordsProducer.stop();
        }
    }

    @Test
    public void shouldReceiveChangesForInsertsWithDifferentDataTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        consumer = testConsumer(1);
        recordsProducer.start(consumer, blackHole);

        try {
            TestHelper.execute("DROP PUBLICATION dbz_publication;");
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        TestHelper.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES;");

        //numerical types
        assertInsert(INSERT_NUMERIC_TYPES_STMT, 1, schemasAndValuesForNumericType());

        //numerical decimal types
        consumer.expects(1);
        assertInsert(INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN, 1, schemasAndValuesForBigDecimalEncodedNumericTypes());

        // string types
        consumer.expects(1);
        assertInsert(INSERT_STRING_TYPES_STMT, 1, schemasAndValuesForStringTypes());

        // monetary types
        consumer.expects(1);
        assertInsert(INSERT_CASH_TYPES_STMT, 1, schemaAndValuesForMoneyTypes());

        // bits and bytes
        consumer.expects(1);
        assertInsert(INSERT_BIN_TYPES_STMT, 1, schemaAndValuesForBinTypes());

        //date and time
        consumer.expects(1);
        assertInsert(INSERT_DATE_TIME_TYPES_STMT, 1, schemaAndValuesForDateTimeTypes());

        // text
        consumer.expects(1);
        assertInsert(INSERT_TEXT_TYPES_STMT, 1, schemasAndValuesForTextTypes());

        // geom types
        consumer.expects(1);
        assertInsert(INSERT_GEOM_TYPES_STMT, 1, schemaAndValuesForGeomTypes());

        // range types
        consumer.expects(1);
        assertInsert(INSERT_RANGE_TYPES_STMT, 1, schemaAndValuesForRangeTypes());
    }


    private void setupRecordsProducer(PostgresConnectorConfig config) {
        if (recordsProducer != null) {
            recordsProducer.stop();
        }

        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);

        PostgresTaskContext context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );
        recordsProducer = new RecordsStreamProducer(context, new SourceInfo(config));
    }

    private void assertInsert(String statement, Integer pk, List<SchemaAndValueField> expectedSchemaAndValuesByColumn) {
        TableId table = tableIdFromInsertStmt(statement);
        String expectedTopicName = table.schema() + "." + table.table();
        expectedTopicName = expectedTopicName.replaceAll("[ \"]", "_");

        try {
            executeAndWait(statement);
            SourceRecord record = assertRecordInserted(expectedTopicName, pk != null ? PK_FIELD : null, pk);
            assertRecordOffsetAndSnapshotSource(record, false, false);
            assertSourceInfo(record, "postgres", table.schema(), table.table());
            assertRecordSchemaAndValues(expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SourceRecord assertRecordInserted(String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());

        if (pk != null) {
            VerifyRecord.isValidInsert(insertedRecord, pkColumn, pk);
        }
        else {
            VerifyRecord.isValidInsert(insertedRecord);
        }

        return insertedRecord;
    }

    private void executeAndWait(String statements) throws Exception {
        TestHelper.execute(statements);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
    }
}
