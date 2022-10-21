using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;
using TheWheel.Domain;
using TheWheel.ETL.DacPac;
using System.Threading;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Configuration;

namespace TheWheel.ETL.Owin
{
    public class Middleware : BaseMiddleware<DbQuery>
    {
        public string[] IgnoredSchema { get; private set; }

        private static ReaderWriterLockSlim lockObject = new ReaderWriterLockSlim();

        public Middleware(IAsyncNewQueryable<DbQuery> provider, IPolicyProvider config)
        : base(provider)
        {
            this.policyProvider = config;
        }

        public async Task<IDataReader> Get(CancellationToken token, SqlCommand query)
        {
            if (query == null || string.IsNullOrEmpty(query.CommandText))
                throw new ArgumentNullException(nameof(query));

            try
            {
                if (query.Connection.State != ConnectionState.Open)
                    await query.Connection.OpenAsync(token);

                var reader = await query.ExecuteReaderAsync(CommandBehavior.CloseConnection, token);

                return reader;
            }
            catch (SqlException)
            {
                query.Connection.Close();
                query.Connection.Dispose();
                throw;
            }
        }

        private static ColumnModel GetColumn(TableModel model, string column)
        {
            return model.columns.FirstOrDefault(c => StringComparer.InvariantCultureIgnoreCase.Equals(c.Name, column));
        }

        private static ColumnModel[] GetColumns(TableModel model, string column)
        {
            column += ".";
            return model.columns.Where(c => c.Name.StartsWith(column, StringComparison.InvariantCultureIgnoreCase)).ToArray();
        }

        static Bag<string, TableModel> dbModels;
        private readonly IPolicyProvider policyProvider;

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
                        tableModel.columns = JsonConvert.DeserializeObject<ColumnModel[]>(reader.GetString(reader.GetOrdinal("columns"))).ToArray();
                    tableModel.object_id = reader.GetInt32(reader.GetOrdinal("object_id"));
                    if (!reader.IsDBNull(reader.GetOrdinal("parameters")))
                        tableModel.parameters = JsonConvert.DeserializeObject<ParameterModel[]>(reader.GetString(reader.GetOrdinal("parameters")));
                    dbModels.Add(tableModel.name, tableModel);
                }
            }
        }

        public void ClearModelCache(CancellationToken token)
        {
            dbModels = null;
        }

        public Task Model(HttpContext context, CancellationToken token)
        {
            return Format(context, new EnumerableDataProvider<TableModel>(EnsureModels(provider, token).ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                    return policyProvider.AllowedAsync(dbModels.Values);
                else
                    throw t.Exception;
            }).Unwrap()));
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
                case FilterOperator.StartsWith:
                case FilterOperator.EndsWith:
                case FilterOperator.StringContains:
                    return " LIKE ";
                default:
                    throw new NotImplementedException();
            }
        }

        private static IColumn[] CleanupColumns(TableModel model, IColumn[] columns)
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

        public override async Task<DbQuery> GetQuery(HttpContext context, string tableName, string id)
        {
            FilterCriteria[] whereCriteria;
            if (context.Request.Method == "GET" || context.Request.Method == "HEAD" || context.Request.Method == "OPTIONS")
                whereCriteria = null;
            else
            {
                var serializer = new JsonSerializer();

                using (var sr = new StreamReader(context.Request.Body))
                using (var jsonTextReader = new JsonTextReader(sr))
                    whereCriteria = serializer.Deserialize<FilterCriteria[]>(jsonTextReader);
            }

            return await GetSql(context.RequestAborted, tableName, context.Request.Query.TryGetValue("$select", out var select) ? select.SelectMany(s => s.Split(',').Select(name => new Column { Name = name })).ToArray() : null, id, context.Request.Query, whereCriteria, context.Request.Query.TryGetValue("$top", out var tops) && int.TryParse(tops[0], out var top) ? top : -1, context.Request.Query.TryGetValue("$skip", out var skips) && int.TryParse(skips, out var skip) ? skip : -1, context.Request.Query.TryGetValue("$orderBy", out var orderbies) ? orderbies[0] : null, context.Request.Query.TryGetValue("$summarize", out var summaries) && bool.TryParse(summaries[0], out var summarize) && summarize);
        }

        public Task<DbQuery> GetSql(CancellationToken token, string table, IColumn[] columns = null, string id = null, IQueryCollection queryString = null, FilterCriteria[] where = null, int top = -1, int skip = 0, string orderby = "", bool count = false)
        {
            return GetSql(token, table, columns, id, queryString == null ? null : queryString.Select(key => new KeyValuePair<string, object>(key.Key, key.Value.Count == 1 ? (object)key.Value[0] : key.Value.ToArray())).ToArray(), where, top, skip, orderby, count);
        }

        public async Task<DbQuery> GetSql(CancellationToken token, string table, IColumn[] columns = null, string id = null, KeyValuePair<string, object>[] queryString = null, FilterCriteria[] where = null, int top = -1, int skip = 0, string orderby = "", bool count = false)
        {
            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            await EnsureModels(provider, token);
            var model = dbModels[table];

            if (model == null)
                return null;

            if (policyProvider != null && !await policyProvider.IsAllowedAsync(model))
                return null;

            DbQueryBuilder query = new DbQueryBuilder();

            columns = CleanupColumns(model, columns);

            IDictionary<string, object> parameters = null;
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

            var whereConditions = new List<StringBuilder>();

            if (id != null)
            {
                var idColumn = model.columns.FirstOrDefault(c => c.IsKey);
                if (idColumn != null)
                    whereConditions.Add(FormatWhere(model, query, new FilterCriteria { PropertyName = idColumn.Name, PropertyValue = id }));
                // whereConditions.Add(new StringBuilder("Id = @Id"));
                // query.Parameters.Add("Id", id);
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
            else
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
                for (int i = 0; i < columns.Length; i++)
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

        private static void WriteValue(StringBuilder sql, DbQueryBuilder query, string type, string name, object value)
        {
            if (value == DBNull.Value || value == null)
            {
                sql.Append("NULL");
                return;
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

        private static StringBuilder Join(string op, IEnumerable<StringBuilder> criteria)
        {
            var sb = new StringBuilder();
            var isFirst = true;
            foreach (var c in criteria)
            {
                if (!isFirst)
                    sb.Append(op);
                else
                    isFirst = false;
                sb.Append(c);
            }
            return sb;
        }

        private static StringBuilder FormatWhere(TableModel model, DbQueryBuilder cmd, TheWheel.Domain.FilterCriteria where)
        {
            if (where.FilterCriterias != null && where.FilterCriterias.Any())
                return Join(FormatOperator(where.FilterOperator), where.FilterCriterias.Select(w => FormatWhere(model, cmd, w)));

            var value = where.PropertyValue;
            var whr = new StringBuilder();
            var column = GetColumn(model, where.PropertyName);
            // if (where.FilterOperator == FilterOperator.Contains)
            //     column.Type = "nvarchar";
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
                    // if (where.IsMultiple)
                    //     switch (column.TypeName)
                    //     {
                    //         case "bit":
                    //             whr.Append('(' + string.Join(",", value.Split('$').Select(s => Convert.ToBoolean(s) ? 1 : 0)) + ')');
                    //             return whr;
                    //         case "tinyint":
                    //             whr.Append('(' + string.Join(",", value.Split('$').Select(s => Convert.ToUInt16(s))) + ')');
                    //             return whr;
                    //         case "int":
                    //             whr.Append('(' + string.Join(",", value.Split('$').Select(s => Convert.ToInt32(s))) + ')');
                    //             return whr;
                    //     }

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
    }
}