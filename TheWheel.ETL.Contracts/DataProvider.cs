using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public class DataProvider<TDataReader, TQueryOptions, TTransport> : DataProvider<TTransport>, IAsyncQueryable<TQueryOptions>
    where TDataReader : IDataReader, IConfigurable<TQueryOptions, Task<IDataReader>>, new()
    where TTransport : ITransport
    where TQueryOptions : ITransportable<TTransport>, IConfigurable<TTransport, Task<TQueryOptions>>
    {
        private TQueryOptions options;

        public override Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return new TDataReader().Configure(this.options);
        }

        public Task QueryAsync(TQueryOptions query, CancellationToken token)
        {
            this.options = query;
            if (options.Transport == null && this.Transport != null)
                return options.Configure(this.Transport).ContinueWith(t => this.options = t.Result, token);
            return Task.FromResult(this);
        }
    }

    public abstract class DataProvider<TTransport> : ITransportable<TTransport>
    where TTransport : ITransport
    {
        public DataProvider()
        {

        }

        public DataProvider(TTransport transport)
        {
            this.transport = transport;
        }

        private TTransport transport;

        public TTransport Transport { get => transport; protected set => transport = value; }

        public virtual IDataReader ExecuteReader()
        {
            var t = ExecuteReaderAsync(CancellationToken.None);
            t.Wait();
            return t.Result;
        }

        public abstract Task<IDataReader> ExecuteReaderAsync(CancellationToken token);

        public Task InitializeAsync(TTransport transport)
        {
            if (this.transport != null)
                throw new InvalidOperationException("The provider has already been initialized with another transport");
            this.transport = transport;
            return Task.FromResult(this);
        }
    }

    public class EnumerableDataProvider<T> : IDataProvider
    {
        private Task<IEnumerable<T>> source;

        public EnumerableDataProvider(Task<IEnumerable<T>> source)
        {
            this.source = source;
        }
        public async Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return DataReader.From(await source, token);
        }
    }

    public class SimpleDataProvider : IDataProvider
    {
        private Task<IDataReader> source;

        public SimpleDataProvider(Task<IDataReader> source)
        {
            this.source = source;
        }
        public Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return source;
        }
    }
}