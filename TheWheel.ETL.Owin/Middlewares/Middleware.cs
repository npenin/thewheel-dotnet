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
using TheWheel.ETL.Fluent;
using TheWheel.Domain;
using TheWheel.ETL.DacPac;
using System.Threading;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Configuration;

namespace TheWheel.ETL.Owin
{
    public class Middleware : BaseMiddleware
    {
        public Middleware(Db provider, IPolicyProvider config, IDataFormatter formatter)
        : base(provider, formatter)
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
                    return policyProvider.AllowedAsync(dbModels.Values, OperationType.Read);
                else
                    throw t.Exception;
            }).Unwrap()));
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

            return await new DbQueryHelper(provider, policyProvider).Query(context.RequestAborted,
             tableName,
             context.Request.Query.TryGetValue("$select", out var select) ? select.SelectMany(s => s.Split(',').Select(name => new Column { Name = name })).ToArray() : null,
              id,
               context.Request.Query.ToArray(),
                whereCriteria,
                context.Request.Query.TryGetValue("$top", out var tops) && int.TryParse(tops[0], out var top) ? top : -1,
                 context.Request.Query.TryGetValue("$skip", out var skips) && int.TryParse(skips, out var skip) ? skip : -1,
                  context.Request.Query.TryGetValue("$orderBy", out var orderbies) ? orderbies[0] : null,
                  context.Request.Query.TryGetValue("$summarize", out var summaries) && bool.TryParse(summaries[0], out var summarize) && summarize);
        }


        public async Task Create(HttpContext context, TableModel table)
        {
            if (context.Request.HasJsonContentType())
            {
                if (policyProvider != null && !await policyProvider.IsAllowedAsync(table, OperationType.Create))
                    throw new UnauthorizedAccessException($"Creation is not allowed on {table.name}");

                var body = await Json.From(new StreamTransport(context.Request.Body), context.RequestAborted, "");

                var reader = await body.ExecuteReaderAsync(context.RequestAborted);

                await provider.ReceiveAsync(body, DbReceiveOptions.Insert(table.name, table.columns.Select(c => new SqlBulkCopyColumnMapping(c.Name, c.Name)).ToArray()), context.RequestAborted);
            }
            throw new NotSupportedException("Only JSON content type is supported for create operation.");
        }

        public async Task Update(HttpContext context, TableModel table)
        {
            if (context.Request.HasJsonContentType())
            {
                if (policyProvider != null && !await policyProvider.IsAllowedAsync(table, OperationType.Update))
                    throw new UnauthorizedAccessException($"Update is not allowed on {table.name}");

                IDataProvider body = await Json.From(new StreamTransport(context.Request.Body), context.RequestAborted, "");

                body = await Helper.Rename(Task.FromResult(body), table.columns.Where(c => c.IsKey).Select(c => c.Name).ToDictionary(c => c, c => "Then." + c));

                var reader = await body.ExecuteReaderAsync(context.RequestAborted);

                await provider.ReceiveAsync(body, DbReceiveOptions.Update(table.name, table.columns.Select(c => new SqlBulkCopyColumnMapping(c.Name, c.Name)).ToArray()), context.RequestAborted);
            }
            throw new NotSupportedException("Only JSON content type is supported for update operation.");
        }

        public async Task Delete(HttpContext context, TableModel table)
        {
            if (context.Request.HasJsonContentType())
            {
                if (policyProvider != null && !await policyProvider.IsAllowedAsync(table, OperationType.Delete))
                    throw new UnauthorizedAccessException($"Deletion is not allowed on {table.name}");

                var body = await Json.From(new StreamTransport(context.Request.Body), context.RequestAborted, "");

                var reader = await body.ExecuteReaderAsync(context.RequestAborted);

                await provider.ReceiveAsync(body, new DbReceiveOptions(await new DbQueryHelper(provider, policyProvider).Delete(context.RequestAborted, table.name, (await body.To<FilterCriteria[]>(record =>
                {
                    var result = new List<FilterCriteria>();
                    for (var i = 0; i < record.FieldCount; i++)
                    {
                        var name = record.GetName(i);
                        var value = record.GetValue(i);
                        if (value != null)
                            result.Add(new FilterCriteria { PropertyName = name, PropertyValue = value });
                    }
                    return result.ToArray();
                }, context.RequestAborted))[0])), context.RequestAborted);
            }
            throw new NotSupportedException("Only JSON content type is supported for update operation.");
        }
    }
}