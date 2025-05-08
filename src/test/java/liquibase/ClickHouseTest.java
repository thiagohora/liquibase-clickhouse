/*-
 * #%L
 * Liquibase extension for Clickhouse
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

import liquibase.ext.clickhouse.params.StandaloneConfig;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class ClickHouseTest extends BaseClickHouseTest {


    @BeforeAll
    static void config() throws Exception {
        setConfig(new StandaloneConfig());
    }

    @Container
    private static final ClickHouseContainer clickHouseContainer =
        new ClickHouseContainer("clickhouse/clickhouse-server:24.4.3");

    @Override
    protected void doWithConnection(BaseClickHouseTest.ThrowingConsumer<Connection> consumer) {
        try (Connection connection = clickHouseContainer.createConnection("")) {
            consumer.accept(connection);
        } catch (Exception e) {
            fail(e);
        }
    }
}
