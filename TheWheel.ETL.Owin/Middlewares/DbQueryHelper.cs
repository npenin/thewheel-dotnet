using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.Domain;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.DacPac;
using TheWheel.ETL.Providers;
using TheWheel.ETL.Fluent;
using Microsoft.Extensions.Primitives;

namespace TheWheel.ETL.Owin
{
    public class DbQueryHelper
    {
        private readonly IAsyncNewQueryable<DbQuery> provider;
        private readonly IPolicyProvider? policyProvider;

        private static Bag<string, TableModel>? dbModels = null;

        public DbQueryHelper(IAsyncNewQueryable<DbQuery> provider, IPolicyProvider? policyProvider = null)
        {
            this.provider = provider;
            this.policyProvider = policyProvider;
        }

        public async System.Threading.Tasks.Task<IDataProvider> QueryAsync(DbQuery query, CancellationToken token)
        {
            return await provider.QueryNewAsync(query, token);
        }

        public void ClearModelCache()
        {
            dbModels = null;
        }

        public async Task<IEnumerable<TableModel>> GetModelsAsync(CancellationToken token)
        {
            await EnsureModels(provider, token);
            if (policyProvider != null)
                return await policyProvider.AllowedAsync(dbModels!.Values);
            return dbModels!.Values;
        }

        public Task<DbQuery?> Query(CancellationToken token, string table, IColumn[]? columns = null, string? id = null, KeyValuePair<string, StringValues>[]? queryString = null, FilterCriteria[]? where = null, int top = -1, int skip = 0, string orderby = "", bool count = false)
        {
            return QueryInternal(token, table, columns, id, queryString, where, top, skip, orderby, count);
        }

        internal async Task<DbQuery?> QueryInternal(CancellationToken token, string table, IColumn[]? columns = null, string? id = null, KeyValuePair<string, StringValues>[]? queryString = null, FilterCriteria[]? where = null, int top = -1, int skip = 0, string orderby = "", bool count = false)
        {
            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            await EnsureModels(provider, token);
            var model = dbModels![table];

            if (model == null)
            {
                System.Console.WriteLine($"Model not found for table: {table}");
                return null;
            }

            if (policyProvider != null && !await policyProvider.IsAllowedAsync(model))
                return null;

            DbQueryBuilder query = new DbQueryBuilder();

            columns = CleanupColumns(model, columns);

            IDictionary<string, StringValues>? parameters = null;
            if (queryString != null)
                parameters = queryString.Where(k => k.Key != null).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var hasColumns = false;
            var isFunction = true;
            if (model.columns != null)
            {
                query.Text.Append("SELECT ");

                if (top > -1 && skip <= 0)
                {
                    query.Text.Append(" TOP (@__top) ");
                    query.Parameters.Add("__top", top);
                }

                if (columns != null && columns.Any())
                {
                    for (int i = 0; i < columns.Length; i++)
                    {
                        var column = GetColumn(model, columns[i].Name);
                        if (column != null)
                        {
                            if (hasColumns)
                                query.Text.Append(',');
                            query.Text.Append('[');
                            query.Text.Append(column.Name);
                            query.Text.Append(']');
                            if (!string.IsNullOrEmpty(columns[i].TranslatedName))
                            {
                                query.Text.Append(" AS ");
                                query.Text.Append('[');
                                query.Text.Append(columns[i].TranslatedName.Replace("]", "]]"));
                                query.Text.Append(']');
                            }
                            hasColumns = true;
                        }
                        var nestedColumns = GetColumns(model, columns[i].Name);
                        if (nestedColumns.Length > 0)
                        {
                            foreach (var col in nestedColumns)
                            {
                                if (hasColumns)
                                    query.Text.Append(',');
                                query.Text.Append('[');
                                query.Text.Append(col.Name);
                                query.Text.Append(']');
                                if (!string.IsNullOrEmpty(columns[i].TranslatedName))
                                {
                                    query.Text.Append(" AS ");
                                    query.Text.Append('[');
                                    query.Text.Append(col.Name.Replace(columns[i].Name, columns[i].TranslatedName.Replace("]", "]]")));
                                    query.Text.Append(']');
                                }
                                hasColumns = true;
                            }
                        }
                    }
                }
                if (!hasColumns)
                {
                    if (count)
                        query.Text.Append("Count(1) AS [Count]");
                    else
                        query.Text.Append(" * ");
                }
                else if (count)
                    query.Text.Append(", Count(1) AS [Count]");

                query.Text.Append(" FROM ");
            }
            else
            {
                query.Text.Append("EXEC ");
                isFunction = false;
            }

            query.Text.Append(model.name);

            var whereConditions = new List<StringBuilder?>();

            if (id != null)
            {
                var idColumn = model.columns?.SingleOrDefault(c => c.IsKey);
                if (idColumn != null)
                    whereConditions.Add(FormatWhere(model, query, new FilterCriteria { PropertyName = idColumn.Name, PropertyValue = id }));
            }

            if (model.parameters != null)
            {
                if (isFunction)
                    query.Text.Append('(');
                else
                    query.Text.Append(' ');
                var isFirstParam = true;
                foreach (var param in model.parameters.Where(p => !p.IsOutput))
                {

                    if (parameters == null || !parameters.TryGetValue(param.Name, out var value))
                    {
                        if (isFunction)
                        {
                            if (!isFirstParam && query.Text[query.Text.Length - 1] != ',')
                                query.Text.Append(',');

                            query.Text.Append("DEFAULT");

                            isFirstParam = false;
                        }
                    }
                    else
                    {
                        if (!isFirstParam && query.Text[query.Text.Length - 1] != ',')
                            query.Text.Append(',');

                        isFirstParam = false;

                        if (!isFunction)
                            query.Text.Append('@').Append(param.Name).Append('=');

                        WriteValue(query.Text, query, param.TypeName, param.Name, value);
                    }
                }
                if (isFunction)
                    query.Text.Append(')');

            }
            else if (queryString != null)
                whereConditions.AddRange(queryString.Select(w => FormatWhere(model, query, new FilterCriteria { PropertyName = w.Key, PropertyValue = w.Value })));

            if (where != null)
                whereConditions.AddRange(where.Select(w => FormatWhere(model, query, w)));

            if (whereConditions.Count > 0)
            {
                var hasWhere = false;
                for (int i = 0; i < whereConditions.Count; i++)
                {
                    if (whereConditions[i] == null)
                        continue;
                    if (hasWhere)
                        query.Text.Append(" AND ");
                    else
                    {
                        query.Text.Append(" WHERE ");
                        hasWhere = true;
                    }
                    query.Text.Append(whereConditions[i]);
                }
            }

            if (count && hasColumns)
            {
                query.Text.Append(" GROUP BY ");
                hasColumns = false;
                for (int i = 0; i < columns!.Length; i++)
                {
                    var column = GetColumn(model, columns[i].Name);
                    if (column != null)
                    {
                        if (hasColumns)
                            query.Text.Append(',');
                        hasColumns = true;
                        query.Text.Append('[');
                        query.Text.Append(column.Name);
                        query.Text.Append(']');
                    }
                }
            }

            var hasOrderBy = false;
            if (!string.IsNullOrWhiteSpace(orderby))
            {
                var orderByCols = orderby.Split(',');
                for (int i = 0; i < orderByCols.Length; i++)
                {
                    var item = orderByCols[i];
                    var isDescDirectionSpeficied = item.EndsWith(" desc", StringComparison.InvariantCultureIgnoreCase);
                    var isAscDirectionSpeficied = item.EndsWith(" asc", StringComparison.InvariantCultureIgnoreCase);

                    string columnName;
                    if (isDescDirectionSpeficied)
                        columnName = item.Substring(0, item.Length - 5).Trim();
                    else if (isAscDirectionSpeficied)
                        columnName = item.Substring(0, item.Length - 4).Trim();
                    else
                        columnName = item;
                    var column = GetColumn(model, columnName);
                    if (column != null)
                    {
                        hasOrderBy = true;
                        if (i > 0)
                            query.Text.Append(',');
                        else
                            query.Text.Append(" ORDER BY ");
                        query.Text.Append('[');
                        query.Text.Append(column.Name);
                        query.Text.Append(']');
                        if (isDescDirectionSpeficied)
                            query.Text.Append(" DESC");
                        else
                            query.Text.Append(" ASC");
                    }
                }
            }
            if (!hasOrderBy && skip > 0)
                query.Text.Append(" ORDER BY 1 ASC");      //this is required to be able to use OFFSET 

            if (skip > 0)
            {
                //OFFSET 10 ROWS                -- skip
                query.Text.Append(" OFFSET @__skip ROWS ");
                query.Parameters.Add("__skip", skip);
                //FETCH NEXT 10 ROWS ONLY       --top
                if (top > 0)
                {
                    query.Text.Append(" FETCH NEXT @__top ROWS ONLY ");
                    query.Parameters.Add("__top", top);
                }
            }

            if (top == -1 && skip == 0)
                query.Timeout = 0;
            else
                query.Timeout = 90;

            return query;
        }

        public async Task<DbQuery?> Insert(CancellationToken token, string table, IDataRecord values)
        {
            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            await EnsureModels(provider, token);
            var model = dbModels![table];

            if (model == null)
            {
                System.Console.WriteLine($"Model not found for table: {table}");
                return null;
            }

            if (policyProvider != null && !await policyProvider.IsAllowedAsync(model, OperationType.Create))
                return null;

            DbQueryBuilder query = new DbQueryBuilder();
            query.Text.Append("INSERT INTO ");
            query.Text.Append(table);
            query.Text.Append(" (");
            var keys = values.GetNames();
            for (int i = 0; i < keys.Length; i++)
            {
                if (i > 0)
                    query.Text.Append(", ");
                query.Text.Append(model.columns.First(c => c.Name == keys[i]).Name);
            }
            query.Text.Append(") VALUES (");
            var isFirst = true;
            foreach (var entry in keys)
            {
                var column = model.columns.First(c => c.Name == entry);
                if (isFirst)
                    isFirst = false;
                else
                    query.Text.Append(", ");

                var value = values[entry];


                WriteValue(query.Text, query, column.TypeName, column.Name, value);
            }

            query.Text.Append(");");

            return query;
        }

        public async Task<DbQuery?> Update(CancellationToken token, string table, IDataRecord values, FilterCriteria[]? where = null)
        {
            await EnsureModels(provider, token);

            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            await EnsureModels(provider, token);
            var model = dbModels![table];

            if (model == null)
            {
                System.Console.WriteLine($"Model not found for table: {table}");
                return null;
            }

            if (policyProvider != null && !await policyProvider.IsAllowedAsync(model, OperationType.Update))
                return null;

            var query = new DbQueryBuilder();
            query.Text.Append("UPDATE ");
            query.Text.Append(table);
            query.Text.Append(" SET ");
            var isFirst = true;
            var keys = values.GetNames();
            foreach (var entry in keys)
            {
                var column = model.columns.First(c => c.Name == entry);
                var value = values[entry];
                if (isFirst)
                    isFirst = false;
                else
                    query.Text.Append(", ");
                query.Text.Append('[');
                query.Text.Append(column.Name);
                query.Text.Append("] = ");
                WriteValue(query.Text, query, column.TypeName, column.Name, value);
            }

            if (where != null && where.Length > 0)
            {
                var hasWhere = false;
                for (int i = 0; i < where.Length; i++)
                {
                    if (where[i] == null)
                        continue;
                    if (hasWhere)
                        query.Text.Append(" AND ");
                    else
                    {
                        query.Text.Append(" WHERE ");
                        hasWhere = true;
                    }
                    query.Text.Append(FormatWhere(model, query, where[i]));
                }
            }

            query.Text.Append(")");

            return query;
        }

        public async Task<DbQuery?> Delete(CancellationToken token, string table, FilterCriteria[]? where = null)
        {
            await EnsureModels(provider, token);

            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            await EnsureModels(provider, token);
            var model = dbModels![table];

            if (model == null)
            {
                System.Console.WriteLine($"Model not found for table: {table}");
                return null;
            }

            if (policyProvider != null && !await policyProvider.IsAllowedAsync(model, OperationType.Delete))
                return null;

            DbQueryBuilder query = new DbQueryBuilder();
            query.Text.Append("DELETE FROM ");
            query.Text.Append(table);

            if (where != null && where.Length > 0)
            {
                query.Text.Append(" WHERE ");

                var hasWhere = false;
                for (int i = 0; i < where.Length; i++)
                {
                    if (where[i] == null)
                        continue;
                    if (hasWhere)
                        query.Text.Append(" AND ");
                    else
                    {
                        query.Text.Append(" WHERE ");
                        hasWhere = true;
                    }
                    query.Text.Append(FormatWhere(model, query, where[i]));
                }
            }

            query.Text.Append(")");

            return query;
        }

        #region Static helpers

        private static async Task EnsureModels(IAsyncNewQueryable<DbQuery> provider, CancellationToken token)
        {
            if (dbModels != null)
                return;

            var cmd = await provider.QueryNewAsync(@"SELECT t.object_id, t.type, SCHEMA_NAME(t.schema_id)+'.'+ t.name name, (SELECT c.name as [name], types.name [typename], CAST(CASE WHEN c.column_id IN (
SELECT column_id FROM sys.key_constraints kc
INNER JOIN sys.index_columns ic ON kc.parent_object_id=ic.object_id AND ic.index_id=kc.unique_index_id
WHERE type='PK' AND parent_object_id=c.object_id) THEN 1 ELSE 0 END as BIT) iskey FROM sys.columns c 
INNER JOIN sys.types types ON c.user_type_id=types.user_type_id
WHERE c.object_id=t.object_id FOR JSON PATH) columns
    , (SELECT RIGHT(p.name, LEN(p.name)-1) name, p.is_output, types.name [typename] FROM sys.parameters p 
INNER JOIN sys.types types ON p.user_type_id=types.user_type_id
	WHERE p.object_id=t.object_id
     FOR JSON PATH) parameters FROM sys.objects t
    INNER JOIN sys.schemas s ON s.schema_id=t.schema_id
        WHERE t.type IN ('U','V', 'P', 'TF', 'X', 'IF')", token);
            using (var reader = await cmd.ExecuteReaderAsync(token))
            {
                dbModels = new Bag<string, TableModel>();
                while (reader.Read())
                {
                    var tableModel = new TableModel();
                    tableModel.name = reader.GetString(reader.GetOrdinal("name"));
                    tableModel.type = reader.GetString(reader.GetOrdinal("type"));
                    if (!reader.IsDBNull(reader.GetOrdinal("columns")))
                        tableModel.columns = (await (await Json.FromString(reader.GetString(reader.GetOrdinal("columns")), token)).To<ColumnModel>(null, token)).ToArray();
                    tableModel.object_id = reader.GetInt32(reader.GetOrdinal("object_id"));
                    if (!reader.IsDBNull(reader.GetOrdinal("parameters")))
                        tableModel.parameters = (await (await Json.FromString(reader.GetString(reader.GetOrdinal("parameters")), token)).To<ParameterModel>(null, token)).ToArray();
                    dbModels.Add(tableModel.name, tableModel);
                }
            }
        }

        private static ColumnModel? GetColumn(TableModel model, string column)
        {
            return model.columns.FirstOrDefault(c => StringComparer.InvariantCultureIgnoreCase.Equals(c.Name, column));
        }

        private static ColumnModel[] GetColumns(TableModel model, string column)
        {
            column += ".";
            return model.columns.Where(c => c.Name.StartsWith(column, StringComparison.InvariantCultureIgnoreCase)).ToArray();
        }

        private static IColumn[]? CleanupColumns(TableModel model, IColumn[]? columns)
        {
            if (columns == null)
                return null;
            var cleanColumns = new List<IColumn>(columns.Length);
            foreach (var c in columns.OrderBy(c => c.Name))
            {
                if (!c.Name.Contains('.') || !c.Name.StartsWith(cleanColumns[cleanColumns.Count - 1].Name))
                    cleanColumns.Add(c);
            }
            return cleanColumns.OrderBy(c => Array.IndexOf(columns, c)).ToArray();
        }

        private static string FormatOperator(FilterOperator filterOperator)
        {
            switch (filterOperator)
            {
                case FilterOperator.Equal:
                    return "=";
                case FilterOperator.Not:
                    return "!=";
                case FilterOperator.Contains:
                    return " LIKE ";
                case FilterOperator.Greater:
                    return " > ";
                case FilterOperator.Lower:
                    return " < ";
                case FilterOperator.GreaterOrEqual:
                    return " >= ";
                case FilterOperator.LowerOrEqual:
                    return " <= ";
                case FilterOperator.Or:
                    return " OR ";
                case FilterOperator.And:
                    return " AND ";
                case FilterOperator.StartsWith:
                case FilterOperator.EndsWith:
                case FilterOperator.StringContains:
                    return " LIKE ";
                default:
                    throw new NotImplementedException();
            }
        }

        private static void WriteValue(StringBuilder sql, DbQueryBuilder query, string type, string name, object? value)
        {
            if (value == DBNull.Value || value == null)
            {
                sql.Append("NULL");
                return;
            }

            if (value is StringValues sv)
            {
                if (sv.Count == 1)
                    value = sv[0];
                else
                    value = sv.ToArray();
            }
            switch (type)
            {
                case "tinyint":
                    sql.Append(Convert.ToUInt16(value));
                    break;
                case "int":
                    sql.Append(Convert.ToInt32(value));
                    break;
                case "datetime":
                    sql.Append('@').Append(name);
                    query.Parameters.Add(name, Convert.ToDateTime(value));
                    break;
                case "bit":
                    if (value is string s)
                        switch (s.ToLower())
                        {
                            case "true":
                                sql.Append(1);
                                break;
                            case "false":
                                sql.Append(0);
                                break;
                            default:
                                sql.Append(short.Parse(s));
                                break;
                        }
                    else
                        sql.Append(Convert.ToBoolean(value) ? 1 : 0);
                    break;
                default:
                    if (value is int[] a)
                    {
                        sql.Append('\'').Append(string.Join(", ", a)).Append('\'');
                    }
                    else
                    {
                        sql.Append('@').Append(name);
                        query.Parameters.Add(name, value == null ? DBNull.Value : value);
                    }
                    break;
            }
        }

        private static StringBuilder Join(string op, IEnumerable<StringBuilder?> criteria)
        {
            var sb = new StringBuilder();
            var isFirst = true;
            foreach (var c in criteria)
            {
                if (c == null)
                    continue;
                if (!isFirst)
                    sb.Append(op);
                else
                    isFirst = false;
                sb.Append(c);
            }
            return sb;
        }

        private static StringBuilder? FormatWhere(TableModel model, DbQueryBuilder cmd, FilterCriteria where)
        {
            if (where.FilterCriterias != null && where.FilterCriterias.Any())
            {
                var text = Join(FormatOperator(where.FilterOperator), where.FilterCriterias.Select(w => FormatWhere(model, cmd, w)));
                text.Insert(0, '(');
                text.Append(')');
                return text;
            }

            var value = where.PropertyValue;
            var whr = new StringBuilder();
            var column = GetColumn(model, where.PropertyName);
            if (column == null)
                return null;

            switch (column.TypeName)
            {
                case "bit":
                case "tinyint":
                case "int":
                    if (where.FilterOperator == FilterOperator.StringContains)
                        where.FilterOperator = FilterOperator.Equal;
                    break;
            }

            whr.Append('[');
            whr.Append(column.Name);
            whr.Append(']');
            whr.Append(FormatOperator(where.FilterOperator));

            switch (where.FilterOperator)
            {
                case FilterOperator.Contains:
                    cmd.Parameters.Add("_p" + cmd.Parameters.Count, value);
                    whr.Append("(SELECT VALUE FROM STRING_SPLIT(@_p");
                    whr.Append((cmd.Parameters.Count - 1));
                    whr.Append(",'$'))");
                    break;
                case FilterOperator.StringContains:
                    cmd.Parameters.Add("_p" + cmd.Parameters.Count, value);
                    whr.Append("'%' + @_p");
                    whr.Append((cmd.Parameters.Count - 1));
                    whr.Append(" + '%'");
                    break;
                case FilterOperator.StartsWith:
                    cmd.Parameters.Add("_p" + cmd.Parameters.Count, value);
                    whr.Append("'%' + @_p");
                    whr.Append((cmd.Parameters.Count - 1));
                    break;
                case FilterOperator.EndsWith:
                    cmd.Parameters.Add("_p" + cmd.Parameters.Count, value);
                    whr.Append("@_p");
                    whr.Append((cmd.Parameters.Count - 1));
                    whr.Append(" + '%'");
                    break;
                default:
                    WriteValue(whr, cmd, column.TypeName, "_p" + cmd.Parameters.Count, value);
                    break;
            }
            return whr;
        }

        #endregion
    }
}
