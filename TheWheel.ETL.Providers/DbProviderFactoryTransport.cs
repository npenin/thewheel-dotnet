using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class DbProviderFactoryTransport : ITransport<IDbCommand>
    {
        private DbProviderFactory factory;
        protected IDbConnection connection;
        protected IDbCommand command;
        public CommandBehavior? Behavior { get; private set; }
        private IDbTransaction transaction;

        public IDbTransaction Transaction => transaction;

        public DbProviderFactoryTransport(DbProviderFactory factory)
        {
            this.factory = factory;
        }

        public void Dispose()
        {
            if (transaction != null)
            {
                transaction.Rollback();
                transaction.Dispose();
            }
            if (connection != null)
                connection.Dispose();
            factory = null;
        }

        public virtual async Task<IDbCommand> GetStreamAsync(CancellationToken token)
        {
            await Task.Run(command.Connection.Open, token);
            transaction = connection.BeginTransaction();
            return command;
        }

        public IDbConnection Connection => connection;

        public Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            connection = factory.CreateConnection();
            connection.ConnectionString = connectionString;
            if (parameters != null && parameters.Length > 0)
            {
                var builder = factory.CreateConnectionStringBuilder();
                builder.ConnectionString = connectionString;
                foreach (var parameter in parameters)
                    builder.Add(parameter.Key, parameter.Value);
                connection.ConnectionString = builder.ConnectionString;
            }
            return Task.CompletedTask;
        }

        public virtual async Task<DbProviderFactoryTransport> QueryNewAsync(DbQuery query, CancellationToken token)
        {
            var newTransport = new DbProviderFactoryTransport(factory);
            newTransport.connection = connection;
            await newTransport.QueryAsync(query, token);
            return newTransport;
        }

        public async Task QueryAsync(DbQuery query, CancellationToken token)
        {
            command = await QueryAsyncInternal(query, token);
            Behavior = query.Behavior;
        }

        public Task<IDbCommand> QueryAsyncInternal(string query, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            return QueryAsyncInternal(new DbQuery(query, parameters), token);
        }

        public Task<IDbCommand> QueryAsyncInternal(DbQuery query, CancellationToken token)
        {
            return Task.Run(() =>
            {
                var command = connection.CreateCommand();
                command.Connection = connection;
                command.Transaction = query.Transaction ?? transaction;
                command.CommandText = query.Text;
                command.CommandTimeout = query.Timeout ?? 30;
                if (query.Parameters != null && query.Parameters.Length > 0)
                {
                    foreach (var parameter in query.Parameters)
                    {
                        var p = command.CreateParameter();
                        p.ParameterName = parameter.Key;
                        p.Value = parameter.Value;
                        command.Parameters.Add(p);
                    }
                }
                return command;
            });
        }
    }
}