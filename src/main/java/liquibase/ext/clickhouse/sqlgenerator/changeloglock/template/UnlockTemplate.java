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
package liquibase.ext.clickhouse.sqlgenerator.changeloglock.template;

import liquibase.database.Database;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.StandaloneConfig;
import liquibase.ext.clickhouse.sqlgenerator.LiquibaseSqlTemplate;

public class UnlockTemplate extends LiquibaseSqlTemplate<String> {

    private final Database database;

    public UnlockTemplate(Database database) {
        this.database = database;
    }

    @Override
    public String visit(ClusterConfig object) {
        return String.format(
            "INSERT INTO %s.%s (ID, LOCKED, LOCKEDBY, LOCKGRANTED)" + "VALUES (1, 0, null, null)",
            database.getLiquibaseCatalogName(), database.getDatabaseChangeLogLockTableName()
        );
    }

    @Override
    public String visit(StandaloneConfig object) {
        return String.format(
            "INSERT INTO `%s`.%s (ID, LOCKED, LOCKEDBY, LOCKGRANTED, SIGN) "
                + "VALUES (1, 1, null, null, -1)",
            database.getLiquibaseCatalogName(), database.getDatabaseChangeLogLockTableName()
        );
    }
}
