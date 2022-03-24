using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{

    public class LazyReceiver<TOptions> : ILazyReceiver
    {
        private readonly IDataReceiver<TOptions> receiver;
        private readonly TOptions options;

        public LazyReceiver(IDataReceiver<TOptions> receiver, TOptions options)
        {
            this.receiver = receiver;
            this.options = options;
        }

        public Task ReceiveAsync(IDataProvider provider, CancellationToken token)
        {
            return receiver.ReceiveAsync(provider, options, token);
        }
    }

    public class ActionReceiver : IDataReceiver<Action<IDataRecord>>, IDataReceiver<Func<IDataRecord, Task>>
    {
        public static ILazyReceiver Lazy(Action<IDataRecord> query)
        {
            return new LazyReceiver<Action<IDataRecord>>(new ActionReceiver(), query);
        }
        public static ILazyReceiver Lazy(Func<IDataRecord, Task> query)
        {
            return new LazyReceiver<Func<IDataRecord, Task>>(new ActionReceiver(), query);
        }

        public async Task ReceiveAsync(IDataProvider provider, Action<IDataRecord> query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                if (reader is IEnumerator<IDataRecord> enumReader)
                {
                    while (enumReader.MoveNext() && !token.IsCancellationRequested)
                        query(enumReader.Current);
                }
                else
                {
                    while (reader.Read() && !token.IsCancellationRequested)
                        query(new DataRecord(reader));
                }
            }
        }

        public async Task ReceiveAsync(IDataProvider provider, Func<IDataRecord, Task> query, CancellationToken token)
        {
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                if (reader is IEnumerator<IDataRecord> enumReader)
                {
                    while (enumReader.MoveNext() && !token.IsCancellationRequested)
                        await query(enumReader.Current);
                }
                else
                {
                    while (reader.Read() && !token.IsCancellationRequested)
                        await query(new DataRecord(reader));
                }
            }
        }
    }
}