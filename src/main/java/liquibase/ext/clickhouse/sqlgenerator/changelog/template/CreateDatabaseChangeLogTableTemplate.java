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
package liquibase.ext.clickhouse.sqlgenerator.changelog.template;

import liquibase.database.Database;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.LiquibaseClickHouseConfig;
import liquibase.ext.clickhouse.params.StandaloneConfig;
import liquibase.ext.clickhouse.sqlgenerator.LiquibaseSqlTemplate;
import liquibase.ext.clickhouse.sqlgenerator.OnClusterTemplate;

import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.AUTHOR;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.COMMENTS;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.CONTEXTS;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.DATEEXECUTED;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.DEPLOYMENT_ID;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.DESCRIPTION;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.EXECTYPE;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.FILENAME;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.ID;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.LABELS;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.LIQUIBASE;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.MD5SUM;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.ORDEREXECUTED;
import static liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns.TAG;

public class CreateDatabaseChangeLogTableTemplate extends LiquibaseSqlTemplate<String> {

    private final Database database;
    private final OnClusterTemplate onClusterVisitor;

    public CreateDatabaseChangeLogTableTemplate(Database database) {
        this.database = database;
        this.onClusterVisitor = new OnClusterTemplate();
    }

    private String generateFirstPart(LiquibaseClickHouseConfig clickHouseConfig) {

        return String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.%s "
                + clickHouseConfig.accept(onClusterVisitor)
                + "("
                + ID
                + " String,"
                + AUTHOR
                + " String,"
                + FILENAME
                + " String,"
                + DATEEXECUTED
                + " DateTime64,"
                + ORDEREXECUTED
                + " UInt64,"
                + EXECTYPE
                + " String,"
                + MD5SUM
                + " Nullable(String),"
                + DESCRIPTION
                + " Nullable(String),"
                + COMMENTS
                + " Nullable(String),"
                + TAG
                + " Nullable(String),"
                + LIQUIBASE
                + " Nullable(String),"
                + CONTEXTS
                + " Nullable(String),"
                + LABELS
                + " Nullable(String),"
                + DEPLOYMENT_ID
                + " Nullable(String)) ",
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogTableName()
        );
    }

    @Override
    public String visit(StandaloneConfig standaloneConfig) {
        return generateFirstPart(standaloneConfig)
                   + String.format(
            "ENGINE = ReplacingMergeTree() ORDER BY (%s, %s, %s)", ID, AUTHOR, FILENAME);
    }

    @Override
    public String visit(ClusterConfig clusterConfig) {
        return generateFirstPart(clusterConfig)
                   + String.format(
            "ENGINE = KeeperMap('%s/%s') PRIMARY KEY (%s)",
            clusterConfig.tableZooKeeperPathPrefix(), database.getDatabaseChangeLogTableName(), ID
        );
    }
}
