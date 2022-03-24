using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
// #if NET5_0
using Microsoft.Extensions.Logging;
using System.Threading;
// #else
// using System.Diagnostics;
// #endif

namespace TheWheel.ETL.Providers
{
    public class Sql : Db, IDataReceiver<SqlReceiveOptions>
    {
        public Sql()
        : this(new SqlProviderFactoryTransport())
        {
        }

        private Sql(DbProviderFactoryTransport transport)
        : base(transport)
        {

        }

        public override async Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            var cmd = (SqlCommand)(this.command = await Transport.GetStreamAsync());
            return await cmd.ExecuteReaderAsync(token);
        }

        public static async Task<Sql> To(string connectionString)
        {
            var sql = new Sql();
            await sql.Transport.InitializeAsync(connectionString);
            return sql;
        }
        public static async Task<Sql> From(string connectionString)
        {
            var sql = new Sql();
            await sql.Transport.InitializeAsync(connectionString);
            return sql;
        }

        public static async Task<IDataRecord> LoadSingle(string connectionString, DbQuery query, CancellationToken token)
        {
            using (var sql = await From(connectionString))
            {
                await sql.QueryAsync(query, token);
                using (var reader = await sql.ExecuteReaderAsync(token))
                    if (reader.Read())
                        return new DataRecord(reader);
            }
            return null;
        }

        public override async Task<IDataProvider> QueryNewAsync(DbQuery query, CancellationToken token)
        {
            return new Sql(await Transport.QueryNewAsync(query, token));
        }

        public async Task ReceiveAsync(IDataProvider provider, SqlReceiveOptions options, CancellationToken token)
        {
            var connection = (SqlConnection)Transport.Connection;
            // if (connection.State == ConnectionState.Closed)
            //     await connection.OpenAsync();
            var copy = new SqlBulkCopy(connection.ConnectionString, options.Bulk);
            // {
            copy.DestinationTableName = options.TableName;
            if (options.BatchSize.HasValue)
                copy.BatchSize = options.BatchSize.Value;
            if (options.Timeout.HasValue)
                copy.BulkCopyTimeout = options.Timeout.Value;
            copy.EnableStreaming = true;
            if (options.Mapping != null)
                foreach (var mapping in options.Mapping)
                    copy.ColumnMappings.Add(mapping);
            var reader = await provider.ExecuteReaderAsync(token);
            if (reader.FieldCount > 0)
                await copy.WriteToServerAsync(reader, token);
            if (Transport.Transaction != null)
                Transport.Transaction.Commit();
            // }
        }


        public override async Task ReceiveAsync(IDataProvider provider, DbReceiveOptions query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                await Transport.QueryAsync(query.query, token);

                var cmd = await Transport.GetStreamAsync();

                if (query.mapping != null)
                {
                    for (int i = 0; i < query.mapping.Length; i++)
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = "__p" + i;
                        cmd.Parameters.Add(param);
                    }
                }

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
                    // #if NET5_0
                    trace.LogInformation(cmd.CommandText);
                    if (trace.IsEnabled(LogLevel.Debug))
                    {
                        foreach (IDbDataParameter param in cmd.Parameters)
                            trace.LogDebug("{0}: {1}", param.ParameterName, param.Value);
                    }
                    // #else
                    //                     trace.TraceEvent(TraceEventType.Information, 100, cmd.CommandText);
                    //                     if (trace.Switch.ShouldTrace(TraceEventType.Verbose))
                    //                     {
                    //                         foreach (IDbDataParameter param in cmd.Parameters)
                    //                             trace.TraceData(TraceEventType.Verbose, 100, string.Format("{0}: {1}", param.ParameterName, param.Value));
                    //                     }
                    // #endif

                    await ((SqlCommand)cmd).ExecuteNonQueryAsync();
                }
            }
        }

    }

    public class SqlReceiveOptions
    {
        public SqlReceiveOptions(SqlReceiveOptions options, params SqlBulkCopyColumnMapping[] mapping)
        : this(options.TableName, new List<SqlBulkCopyColumnMapping>(mapping))
        {
            this.Bulk = options.Bulk;
            this.Transaction = options.Transaction;
            this.BatchSize = options.BatchSize;
            this.Timeout = options.Timeout;
        }
        public SqlReceiveOptions(SqlReceiveOptions options, IList<SqlBulkCopyColumnMapping> mapping)
        : this(options.TableName, mapping)
        {
            this.Bulk = options.Bulk;
            this.Transaction = options.Transaction;
            this.BatchSize = options.BatchSize;
            this.Timeout = options.Timeout;
        }
        public SqlReceiveOptions(string tableName, params SqlBulkCopyColumnMapping[] mapping)
        : this(tableName, new List<SqlBulkCopyColumnMapping>(mapping))
        {
        }

        public SqlReceiveOptions(string tableName, IList<SqlBulkCopyColumnMapping> mapping)
        {
            this.TableName = tableName;
            this.Mapping = mapping;
        }


        public SqlBulkCopyOptions Bulk;

        public SqlTransaction Transaction;
        public int? BatchSize;
        public int? Timeout;

        public string TableName { get; }
        public IList<SqlBulkCopyColumnMapping> Mapping;
    }


    public class SqlProviderFactoryTransport : DbProviderFactoryTransport, ITransport<SqlCommand>
    {
        public SqlProviderFactoryTransport()
        : base(SqlClientFactory.Instance)
        {

        }

        public override async Task<IDbCommand> GetStreamAsync()
        {
            return await ((ITransport<SqlCommand>)this).GetStreamAsync();
        }

        async Task<SqlCommand> ITransport<SqlCommand>.GetStreamAsync()
        {
            var cmd = (SqlCommand)command;
            if (cmd.Connection.State == ConnectionState.Closed)
                await cmd.Connection.OpenAsync();
            return cmd;
        }

        public override async Task<DbProviderFactoryTransport> QueryNewAsync(DbQuery query, CancellationToken token)
        {
            var newTransport = new SqlProviderFactoryTransport();
            newTransport.connection = connection;
            await newTransport.QueryAsync(query, token);
            return newTransport;
        }

    }
}
