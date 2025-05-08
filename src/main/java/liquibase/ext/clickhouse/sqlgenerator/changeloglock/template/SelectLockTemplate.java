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

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.StandaloneConfig;
import liquibase.ext.clickhouse.sqlgenerator.LiquibaseSqlTemplate;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectLockTemplate extends LiquibaseSqlTemplate<String> {

    private final Database database;
    private final SelectFromDatabaseChangeLogLockStatement statement;

    public SelectLockTemplate(Database database, SelectFromDatabaseChangeLogLockStatement statement) {
        this.database = database;
        this.statement = statement;
    }

    @Override
    public String visit(ClusterConfig object) {
        String selector =
            Arrays.stream(statement.getColumnsToSelect())
                .map(ColumnConfig::getName)
                .collect(Collectors.joining(", "));
        return String.format(
            "SELECT %s FROM %s.%s WHERE ID = 1;",
            selector, database.getLiquibaseCatalogName(), database.getDatabaseChangeLogLockTableName()
        );
    }

    @Override
    public String visit(StandaloneConfig object) {
        String selector =
            Arrays.stream(statement.getColumnsToSelect())
                .map(ColumnConfig::getName)
                .map(it -> it.equals("LOCKED") ? "max(LOCKED)" : it)
                .collect(Collectors.joining(", "));
        String groupBy = getGroupBy(selector);
        return String.format(
            "SELECT %s FROM %s.%s FINAL WHERE ID = 1 %s;",
            selector,
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogLockTableName(),
            getGroupBy(selector)
        );
    }

    private String getGroupBy(String selector) {
        String args =
            Stream.of(selector.split(", "))
                .filter(it -> !it.equals("max(LOCKED)"))
                .collect(Collectors.joining(", "));
        if (StringUtils.isBlank(args)) {
            return "";
        }
        return "GROUP BY " + args;
    }
}
