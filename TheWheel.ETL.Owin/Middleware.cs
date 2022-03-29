using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Owin;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;
using TheWheel.Domain;

namespace TheWheel.ETL.Owin
{
    public class Middleware : OwinMiddleware
    {
        private string connectionString;

        public static Dictionary<string, Func<IDataProvider, Stream, Task>> Formatters { get; } = new Dictionary<string, Func<IDataProvider, Stream, Task>>();

        public static void AddJsonFormatter()
        {
            RegisterReveiverFormatter<Json, TreeOptions>("application/json");
            RegisterReveiverFormatter<Json, TreeOptions>("text/json");
        }

        public static void AddCsvFormatter()
        {
            RegisterReveiverFormatter<Csv, CsvReceiverOptions>("text/csv");
        }

        public static void RegisterReveiverFormatter<T, TOptions>(string mediaType)
        where T : IDataReceiver<TOptions>, new()
        where TOptions : IConfigurable<ITransport<Stream>, Task<TOptions>>, new()
        {
            Formatters.Add(mediaType, async (provider, stream) =>
             {
                 var receiver = new T();
                 var options = new TOptions();
                 await options.Configure(new StreamTransport().Configure(stream));
                 await receiver.ReceiveAsync(provider, options, new System.Threading.CancellationTokenSource().Token);
             });
        }


        public Middleware(string connectionString, OwinMiddleware next)
        : base(next)
        {
            this.connectionString = connectionString;
        }

        public override Task Invoke(IOwinContext context)
        {
            if (context.Request.Path.StartsWithSegments(new PathString("/model")))
                return Format(context, new EnumerableDataProvider<TableModel>(Model()));
            if (context.Request.Path.StartsWithSegments(new PathString("/data/"), out var table))
            {
                var indexOfSlash = -1;
                if (table.HasValue)
                    indexOfSlash = table.Value.IndexOf('/');
                string tableName;
                if (indexOfSlash > -1)
                    tableName = table.Value.Substring(0, indexOfSlash);
                else
                    tableName = table.Value;

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
                return Format(context, new SimpleDataProvider(Get(this.connectionString, tableName, context.Request.Query, whereCriteria, int.TryParse(context.Request.Query["top"], out var top) ? top : 10)));
            }
            else
                return Next.Invoke(context);
        }

        private Task Format(IOwinContext context, IDataProvider data)
        {
            var accepts = context.Request.Headers.GetValues("Accept").Select(h => MediaTypeWithQualityHeaderValue.TryParse(h, out var accept) ? accept : null).OrderByDescending(h => h.Quality);

            foreach (var accept in accepts)
            {
                if (Formatters.TryGetValue(accept.MediaType, out var formatter))
                {
                    return formatter(data, context.Response.Body);
                }
            }

            var json = new Json();
            return json.ReceiveAsync(data, new TreeOptions() { Transport = new StreamTransport().Configure(context.Response.Body) }, new System.Threading.CancellationTokenSource().Token);
        }

        static Bag<string, TableModel> dbModels;

        private static async Task EnsureModels(SqlConnection connection)
        {
            if (dbModels != null)
                return;
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"SELECT t.object_id, t.type, SCHEMA_NAME(t.schema_id)+'.'+ t.name name, (SELECT c.name as [column] FROM sys.columns c WHERE c.object_id=t.object_id FOR JSON PATH) columns
    , (SELECT p.name, p.is_output FROM sys.parameters p WHERE p.object_id=t.object_id
     FOR JSON PATH) parameters FROM sys.objects t
    INNER JOIN sys.schemas s ON s.schema_id=t.schema_id
        WHERE t.type IN ('U','V', 'P', 'TF', 'X', 'IF')
        AND s.name NOT IN ('Lookup', 'Report','Stage', 'Ops')";
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    dbModels = new Bag<string, TableModel>();
                    while (await reader.ReadAsync())
                    {
                        var tableModel = new TableModel();
                        tableModel.name = reader.GetString(reader.GetOrdinal("name"));
                        tableModel.type = reader.GetString(reader.GetOrdinal("type"));
                        if (!await reader.IsDBNullAsync(reader.GetOrdinal("columns")))
                            tableModel.columns = JsonConvert.DeserializeAnonymousType(reader.GetString(reader.GetOrdinal("columns")), new[] { new { column = "" } }).Select(t => t.column).ToArray();
                        tableModel.object_id = reader.GetInt32(reader.GetOrdinal("object_id"));
                        if (!await reader.IsDBNullAsync(reader.GetOrdinal("parameters")))
                            tableModel.parameters = JsonConvert.DeserializeObject<ParameterModel[]>(reader.GetString(reader.GetOrdinal("parameters")));
                        dbModels.Add(tableModel.name.ToLower(), tableModel);
                    }
                }
            }
        }

        public async Task<IEnumerable<TableModel>> Model()
        {
            var connection = new SqlConnection(connectionString);
            try
            {
                await connection.OpenAsync();

                await EnsureModels(connection);

                return dbModels.Values;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
        }


        private static string FormatWhere(SqlCommand cmd, FilterCriteria where)
        {
            if (where.FilterCriterias != null)
                return string.Join(FormatOperator(where.Operator), where.FilterCriterias);

            cmd.Parameters.AddWithValue("_p" + cmd.Parameters.Count, where.PropertyValue);

            return where.PropertyName + FormatOperator(where.Operator) + "@_p" + (cmd.Parameters.Count - 1);
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

        public static async Task<IDataReader> Get(string connectionString, string table, IReadableStringCollection queryString = null, FilterCriteria[] where = null, int top = 10, bool considerNonLastImported = false)
        {
            if (string.IsNullOrEmpty(table))
                throw new KeyNotFoundException(table);

            var connection = new SqlConnection(connectionString);
            try
            {
                await connection.OpenAsync();

                await EnsureModels(connection);
                var model = dbModels[table.ToLower()];

                if (model == null)
                    return null;

                SqlCommand query = connection.CreateCommand();
                if (top == -1)
                    query.CommandText = "SELECT * FROM " + model.name;
                else
                {
                    query.CommandText = "SELECT TOP (@__top) * FROM " + model.name;
                    query.Parameters.AddWithValue("__top", top);
                }

                var whereConditions = new List<string>();

                if (model.parameters != null)
                {
                    query.CommandText += "(";
                    var isFirstParam = true;
                    foreach (var param in model.parameters.Where(p => !p.is_output))
                    {
                        if (isFirstParam)
                            isFirstParam = false;
                        else
                            query.CommandText += ",";
                        var value = queryString.Get(param.name);
                        if (value == null)
                            query.CommandText += "DEFAULT";
                        else
                            query.CommandText += "@" + param.name;
                        query.Parameters.AddWithValue(param.name, queryString.Get(param.name));
                    }
                    query.CommandText += ")";

                }
                if (where != null)
                {
                    whereConditions.AddRange(where.Select(w => FormatWhere(query, w)));
                }

                if (whereConditions.Count > 0)
                    query.CommandText += " WHERE " + string.Join(" AND ", whereConditions);

                var reader = await query.ExecuteReaderAsync();

                if (!reader.HasRows)
                    return new EmptyDataReader();

                return reader;
            }
            catch (SqlException)
            {
                connection.Close();
                connection.Dispose();
                throw;
            }
        }
    }
}