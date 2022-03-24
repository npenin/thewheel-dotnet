using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Contracts
{
    public class PassthroughProvider : IDataProvider
    {
        public PassthroughProvider(string name)
        {
            reader = new PassthroughReader(name);
        }
        private SemaphoreSlim waitForPush = new SemaphoreSlim(0, 1);

        public async Task Push(IDataRecord record)
        {
            await reader.Push(record);

            if (waitForPush.CurrentCount == 0)
                waitForPush.Release();
        }

        private readonly PassthroughReader reader;
        public IDataReader ExecuteReader()
        {
            var t = ExecuteReaderAsync(CancellationToken.None);
            t.Wait();
            return t.Result;
        }

        public async Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            await waitForPush.WaitAsync(token);
            return reader;
        }

        public Task<Task> ReceiveAsync(IDataProvider provider, CancellationToken token)
        {
            return provider.ExecuteReaderAsync(token).ContinueWith(async readerTask =>
            {
                var reader = readerTask.Result;
                if (reader is IEnumerator<IDataRecord> enumerator)
                {
                    while (enumerator.MoveNext())
                        await this.Push(enumerator.Current);
                }
                else
                {
                    while (reader.Read())
                        await this.Push(new DataRecord(reader));
                }
                await this.Push(null);
            }, token);
        }
    }
}