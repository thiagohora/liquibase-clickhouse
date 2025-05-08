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
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.StandaloneConfig;
import liquibase.ext.clickhouse.sqlgenerator.LiquibaseSqlTemplate;
import liquibase.ext.clickhouse.sqlgenerator.OnClusterTemplate;
import liquibase.ext.clickhouse.sqlgenerator.changelog.ChangelogColumns;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class UpsertTemplate extends LiquibaseSqlTemplate<String> {

    private static final DataTypeFactory DATA_TYPE_FACTORY = DataTypeFactory.getInstance();

    private final OnClusterTemplate onClusterTemplate = new OnClusterTemplate();
    private final Database database;
    private final Map<ChangelogColumns, Object> replacements;
    private final String unescapedId;

    public UpsertTemplate(
        Database database, Map<ChangelogColumns, Object> replacements, String unescapedId
    ) {
        this.database = database;
        this.replacements = replacements;
        this.unescapedId = unescapedId;
    }

    @Override
    public String visit(ClusterConfig clusterConfig) {
        String alteredColumns =
            replacements.entrySet().stream()
                .map(
                    entry ->
                        String.format("%s = %s", entry.getKey(), escape(database, entry.getValue())))
                .collect(Collectors.joining(", "));
        String whereColumns = String.format("ID = %s", unescapedId);

        return String.format(
            "ALTER TABLE %s.%s %s UPDATE %s WHERE %s",
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogTableName(),
            clusterConfig.accept(onClusterTemplate),
            alteredColumns,
            whereColumns
        );
    }

    @Override
    public String visit(StandaloneConfig standaloneConfig) {
        String selectedColumns =
            Arrays.stream(ChangelogColumns.values())
                .map(
                    it ->
                        replacements.containsKey(it)
                            ? escape(database, replacements.get(it))
                            : it.name())
                .collect(Collectors.joining(", "));
        return String.format(
            "INSERT INTO %s.%s SELECT %s from %s.%s final where ID = %s limit 1;",
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogTableName(),
            selectedColumns,
            database.getLiquibaseCatalogName(),
            database.getDatabaseChangeLogTableName(),
            unescapedId
        );
    }

    private String escape(Database database, Object value) {
        return DATA_TYPE_FACTORY.fromObject(value, database).objectToSql(value, database);
    }
}
