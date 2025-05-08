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
import liquibase.ext.clickhouse.sqlgenerator.OnClusterTemplate;

public class CreateDatabaseChangeLogLockTableTemplate extends LiquibaseSqlTemplate<String> {

    private final Database database;
    private final OnClusterTemplate onClusterTemplate;

    public CreateDatabaseChangeLogLockTableTemplate(Database database) {
        this.database = database;
        this.onClusterTemplate = new OnClusterTemplate();
    }

    @Override
    public String visit(StandaloneConfig standaloneConfig) {
        return String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.%s "
                + "("
                + "ID Int64,"
                + "LOCKED UInt8,"
                + "SIGN Int8,"
                + "LOCKGRANTED Nullable(DateTime64),"
                + "LOCKEDBY Nullable(String)"
                + ") "
                + "ENGINE CollapsingMergeTree(SIGN) ORDER BY (ID, LOCKED)",
            database.getLiquibaseCatalogName(), database.getDatabaseChangeLogLockTableName()
        );
    }

    @Override
    public String visit(ClusterConfig clusterConfig) {
        return String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.%s %s"
                + "("
                + "ID Int64,"
                + "LOCKED UInt8,"
                + "LOCKGRANTED Nullable(DateTime64),"
                + "LOCKEDBY Nullable(String)"
                + ") "
                + "ENGINE KeeperMap('%s/%s', 1) PRIMARY KEY (ID)",
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogLockTableName(),
            clusterConfig.accept(onClusterTemplate),
            clusterConfig.tableZooKeeperPathPrefix(),
            database.getDatabaseChangeLogLockTableName()
        );
    }
}
