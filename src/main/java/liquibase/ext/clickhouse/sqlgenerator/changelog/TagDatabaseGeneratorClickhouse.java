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
package liquibase.ext.clickhouse.sqlgenerator.changelog;

import liquibase.database.Database;
import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.ext.clickhouse.params.LiquibaseClickHouseConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;
import liquibase.ext.clickhouse.sqlgenerator.SqlGeneratorUtil;
import liquibase.ext.clickhouse.sqlgenerator.changelog.template.UpsertTemplate;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.TagDatabaseGenerator;
import liquibase.statement.core.TagDatabaseStatement;

import java.util.EnumMap;

public class TagDatabaseGeneratorClickhouse extends TagDatabaseGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(TagDatabaseStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
        TagDatabaseStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain
    ) {
        LiquibaseClickHouseConfig config = ParamsLoader.getLiquibaseClickhouseProperties();
        var maxIDSubQuery =
            String.format(
                "(SELECT ID FROM %s.%s FINAL LIMIT 1)",
                database.getLiquibaseCatalogName(), database.getDatabaseChangeLogTableName()
            );
        var replacements = new EnumMap<>(ChangelogColumns.class);
        replacements.put(ChangelogColumns.TAG, statement.getTag());
        var query = config.accept(new UpsertTemplate(database, replacements, maxIDSubQuery));
        return SqlGeneratorUtil.generateSql(database, query);
    }
}
