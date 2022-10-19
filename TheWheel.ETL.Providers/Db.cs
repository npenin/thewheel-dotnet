using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using Microsoft.Extensions.Logging;
using System.Threading;

namespace TheWheel.ETL.Providers
{
    public class Db : DataProvider<DbProviderFactoryTransport>, ITransportable<DbProviderFactoryTransport>, IAsyncQueryable<DbQuery>
    , IDataReceiver<DbReceiveOptions>, IAsyncNewQueryable<DbQuery>, IDisposable
    {
        protected static readonly ILogger<Db> trace = Logging.factory.CreateLogger<Db>();

        public static async Task<Db> From(ConnectionStringSettings connectionString, CancellationToken token)
        {
            if (connectionString.ProviderName == null || connectionString.ProviderName == "System.Data.SqlClient")
                return await Sql.From(connectionString.ConnectionString, token);
            var db = new Db(DbProviderFactories.GetFactory(connectionString.ProviderName));
            await db.Transport.InitializeAsync(connectionString.ConnectionString, token);
            return db;
        }

        public Db(DbProviderFactory factory)
        : this(new DbProviderFactoryTransport(factory))
        {
        }

        public Db(DbProviderFactoryTransport factory)
        : base(factory)
        {
            this.Transport = factory;
        }

        protected IDbCommand command;

        public void Dispose()
        {
            Transport.Dispose();
            if (command != null)
                command.Dispose();
        }

        public override async Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            this.command = await Transport.GetStreamAsync(token);
            if (Transport.Behavior.HasValue)
                return command.ExecuteReader(Transport.Behavior.Value);
            return command.ExecuteReader();
        }

        public async Task<int> ExecuteNonQuery(DbQuery query, CancellationToken token)
        {
            using (var cmd = await Transport.QueryAsyncInternal(query, token))
            {
                if (cmd.Connection.State == ConnectionState.Closed)
                    cmd.Connection.Open();
                cmd.CommandTimeout = query.Timeout ?? 30;
                return cmd.ExecuteNonQuery();
            }
        }

        public Task<int> ExecuteNonQuery(string query, params KeyValuePair<string, object>[] parameters)
        {
            return ExecuteNonQuery(new DbQuery(query, parameters), CancellationToken.None);
        }



        public virtual int InsertSingle(string query, params KeyValuePair<string, object>[] parameters)
        {
            return InsertSingle(new DbQuery(query, parameters), CancellationToken.None);
        }

        public virtual int InsertSingle(DbQuery query, CancellationToken token)
        {
            Task<int> t = InsertSingleAsync(query, token);
            t.Wait();
            return t.Result;
        }

        public virtual Task<int> InsertSingleAsync(string query, params KeyValuePair<string, object>[] parameters)
        {
            return InsertSingleAsync(new DbQuery(query, parameters), CancellationToken.None);
        }

        public virtual async Task<int> InsertSingleAsync(DbQuery query, CancellationToken token)
        {
            query = new DbQuery(query, query.Text + "; SELECT SCOPE_IDENTITY()");

            IDbCommand command = await Transport.QueryAsyncInternal(query, token);
            // #if NET5_0_OR_GREATER
            trace.LogTrace("{0} ExecuteNonQuery", "Start");
            // #else
            //             trace.TraceEvent(TraceEventType.Start, 0, "ExecuteNonQuery");
            // #endif
            var result = Task.Run(() =>
            {
                var value = command.ExecuteScalar();
                if (value == DBNull.Value)
                    return 0;
                return Convert.ToInt32(value);
            });
            // #if NET5_0_OR_GREATER
            trace.LogTrace("{0} ExecuteNonQuery", "Stop");
            // #else
            //             trace.TraceEvent(TraceEventType.Stop, 0, "ExecuteNonQuery");
            // #endif
            return await result;
        }

        public Task QueryAsync(DbQuery query, CancellationToken token)
        {
            return Transport.QueryAsync(query, token);
        }

        public virtual async Task<IDataProvider> QueryNewAsync(DbQuery query, CancellationToken token)
        {
            return new Db(await Transport.QueryNewAsync(query, token));
        }

        private object[] GetObjectArrayKey(IDataReader reader, int valueOrdinal)
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            return values.Where((v, i) => i != valueOrdinal).ToArray();

        }

        private object GetObjectKey(IDataReader reader, int valueOrdinal)
        {
            if (reader.FieldCount > 2)
                throw new InvalidOperationException("There can be only 2 fields in the query to have a single object key");

            return reader.GetValue(1 - valueOrdinal);
        }

        public virtual async Task ReceiveAsync(IDataProvider provider, DbReceiveOptions query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                await Transport.QueryAsync(query.query, token);
                var cmd = await Transport.GetStreamAsync(token);

                if (query.mapping != null)
                {
                    for (int i = 0; i < query.mapping.Length; i++)
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = "__p" + i;
                        cmd.Parameters.Add(param);
                    }
                }
                var tasks = new List<Task>();
                while (reader.Read() && !token.IsCancellationRequested)
                {
                    if (query.mapping != null)
                        for (int i = 0; i < query.mapping.Length; i++)
                        {
                            if (string.IsNullOrEmpty(query.mapping[i].SourceColumn))
                                ((IDbDataParameter)cmd.Parameters["__p" + i]).Value = reader.GetValue(query.mapping[i].SourceOrdinal) ?? DBNull.Value;
                            else
                                ((IDbDataParameter)cmd.Parameters["__p" + i]).Value = reader.GetValue(reader.GetOrdinal(query.mapping[i].SourceColumn)) ?? DBNull.Value;
                        }

                    // #if NET5_0_OR_GREATER
                    trace.LogInformation(cmd.CommandText);
                    if (trace.IsEnabled(LogLevel.Debug))
                    {
                        foreach (IDbDataParameter param in cmd.Parameters)
                            trace.LogDebug("{0}: {1}", param.ParameterName, param.Value);
                    }
                    // #else
                    //                     trace.TraceInformation(cmd.CommandText);
                    //                     if (trace.Switch.ShouldTrace(TraceEventType.Verbose))
                    //                     {
                    //                         foreach (IDbDataParameter param in cmd.Parameters)
                    //                             trace.TraceData(TraceEventType.Verbose, 100, string.Format("{0}: {1}", param.ParameterName, param.Value));
                    //                     }
                    // #endif
                    tasks.Add(Task.Run(cmd.ExecuteNonQuery));
                }
                await Task.WhenAll(tasks);
                if (cmd.Transaction != null)
                    cmd.Transaction.Commit();
            }
        }
    }
}
