/*-
 * #%L
 * Liquibase extension for ClickHouse
 * %%
 * Copyright (C) 2020 - 2025 Genestack LTD
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package liquibase;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.ext.clickhouse.params.LiquibaseClickHouseConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.output.NullWriter;

import java.sql.Connection;

public abstract class BaseClickHouseTest {
    @Test
    void canInitializeLiquibaseSchema() {
        runLiquibase("empty-changelog.xml", (liquibase, connection) -> liquibase.update(""));
    }

    @Test
    void canExecuteChangelog() {
        // Expected single row from
        runLiquibase(
            "changelog.xml",
            (liquibase, connection) -> {
                liquibase.update("");
                liquibase.update(""); // Test that successive updates are working
            }
        );
    }

    @Test
    void canRollbackChangelog() {
        runLiquibase(
            "changelog.xml",
            (liquibase, connection) -> {
                liquibase.update("");
                liquibase.rollback(2, "");
            }
        );
    }

    @Test
    void canTagDatabase() {
        runLiquibase(
            "changelog.xml",
            (liquibase, connection) -> {
                liquibase.update("");
                liquibase.tag("testTag");
            }
        );
    }

    @Test
    void canValidate() {
        runLiquibase("changelog.xml", (liquibase, connection) -> liquibase.validate());
    }

    @Test
    void canListLocks() {
        runLiquibase("changelog.xml", (liquibase, connection) -> liquibase.listLocks());
    }

    @Test
    void canSyncChangelog() {
        // ERROR: Exception Primary Reason: Result set larger than one row
        runLiquibase("changelog.xml", (liquibase, connection) -> liquibase.changeLogSync(""));
    }

    @Test
    void canForceReleaseLocks() {
        runLiquibase("changelog.xml", (liquibase, connection) -> liquibase.forceReleaseLocks());
    }

    @Test
    void canReportStatus() {
        runLiquibase(
            "changelog.xml",
            (liquibase, connection) -> liquibase.reportStatus(true, "", new NullWriter())
        );
    }

    @Test
    void canMarkChangeSetRan() {
        runLiquibase("changelog.xml", (liquibase, connection) -> liquibase.markNextChangeSetRan(""));
    }

    protected abstract void doWithConnection(ThrowingConsumer<Connection> callback);

    protected void runLiquibase(
        String changelog, ThrowingBiConsumer<Liquibase, Connection> liquibaseAction
    ) {
        DatabaseFactory dbFactory = DatabaseFactory.getInstance();
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        doWithConnection(
            connection -> {
                JdbcConnection jdbcConnection = new JdbcConnection(connection);
                Database database = dbFactory.findCorrectDatabaseImplementation(jdbcConnection);
                database.setLiquibaseSchemaName("default");
                database.setLiquibaseCatalogName("default");
                Liquibase liquibase = new Liquibase(changelog, resourceAccessor, database);
                liquibaseAction.accept(liquibase, connection);
            });
    }

    protected static void setConfig(LiquibaseClickHouseConfig config)
        throws NoSuchFieldException, IllegalAccessException {
        var f = ParamsLoader.class.getDeclaredField("liquibaseClickhouseProperties");
        f.setAccessible(true);
        f.set(null, config);
    }

    @FunctionalInterface
    protected interface ThrowingBiConsumer<T1, T2> {
        void accept(T1 t1, T2 t2) throws java.lang.Exception;
    }

    @FunctionalInterface
    protected interface ThrowingConsumer<T1> {
        void accept(T1 t1) throws java.lang.Exception;
    }
}
