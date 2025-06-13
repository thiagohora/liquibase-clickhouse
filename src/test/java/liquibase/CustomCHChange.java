/*-
 * #%L
 * Liquibase extension for ClickHouse
 * %%
 * Copyright (C) 2024 - 2025 Genestack Limited
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

import liquibase.change.custom.CustomTaskChange;
import liquibase.change.custom.CustomTaskRollback;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;

import java.sql.Statement;

public class CustomCHChange implements CustomTaskChange, CustomTaskRollback {

    @Override
    public void execute(Database database) throws CustomChangeException {
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO DataByRowDist(rowId, item) VALUES (101, 'custom');");
        } catch (Exception e) {
            throw new CustomChangeException(e.getMessage(), e);
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "Message";
    }

    @Override
    public void setUp() throws SetupException {
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
    }

    @Override
    public ValidationErrors validate(Database database) {
        return null;
    }

    @Override
    public void rollback(Database database) throws CustomChangeException {
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE DataByRowShard ON CLUSTER '{cluster}' " +
                              "DELETE WHERE rowId = 101 AND item = 'custom';");
        } catch (Exception e) {
            throw new CustomChangeException(e.getMessage(), e);
        }
    }
}
