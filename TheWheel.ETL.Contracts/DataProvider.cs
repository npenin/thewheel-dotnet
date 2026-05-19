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
    where TDataReader : IDataReader, IConfigurableAsync<TQueryOptions, IDataReader>, new()
    where TTransport : ITransport
    where TQueryOptions : ITransportable<TTransport>, IConfigurableAsync<TTransport, TQueryOptions>
    {
        private TQueryOptions options;

        public override Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return new TDataReader().Configure(this.options, token);
        }

        public Task QueryAsync(TQueryOptions query, CancellationToken token)
        {
            this.options = query;
            if (options.Transport == null && this.Transport != null)
                return options.Configure(this.Transport, token).ContinueWith(t => this.options = t.Result, token);
            return Task.FromResult(this);
        }

        public async Task<IList<T>> To<T>(Func<IDataRecord, T> map, CancellationToken token)
        {
            var receiver = new EnumerableDataReceiver<T>();
            await receiver.ReceiveAsync(this, map, token);
            return receiver.Values;
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

        public void Initialize(TTransport transport)
        {
            if (this.transport != null)
                throw new InvalidOperationException("The provider has already been initialized with another transport");
            this.transport = transport;
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
    public class EnumerableDataReceiver<T> : IDataReceiver<Func<IDataRecord, T>>
    {
        public async Task ReceiveAsync(IDataProvider provider, Func<IDataRecord, T> query, CancellationToken token)
        {
            var reader = await provider.ExecuteReaderAsync(token);
            Values = new List<T>();
            if (query == null)
                query = record =>
                {
                    var result = Activator.CreateInstance<T>();
                    for (int i = 0; i < record.FieldCount; i++)
                    {
                        var member = typeof(T).GetMember(record.GetName(i));
                        if (member != null)
                        {
                            switch (member[0])
                            {
                                case System.Reflection.PropertyInfo property:
                                    if (property.CanWrite)
                                        property.SetValue(result, record.GetValue(i));
                                    break;
                                case System.Reflection.FieldInfo field:
                                    field.SetValue(result, record.GetValue(i));
                                    break;
                            }
                        }
                    }
                    return result;
                };

            while (reader.Read())
            {
                if (token.IsCancellationRequested)
                    break;
                Values.Add(query(reader));
            }
        }

        public IList<T> Values { get; private set; }
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