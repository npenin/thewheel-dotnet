using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Contracts
{
    public class TransformProvider : IDataProvider, IDataReceiver<Func<IDataRecord, IDataRecord>>
    {
        public TransformProvider()
        {
            this.fieldNames = new string[0];
        }
        public TransformProvider(params string[] fieldNames)
        {
            this.fieldNames = fieldNames;
        }

        private IDataProvider provider;
        private Func<IDataRecord, IDataRecord> transformation;
        private string[] fieldNames;

        public async Task<IDataReader> ExecuteReaderAsync(CancellationToken token)
        {
            return new TransformReader(await provider.ExecuteReaderAsync(token), fieldNames, transformation);
        }

        public Task ReceiveAsync(IDataProvider provider, Func<IDataRecord, IDataRecord> query, CancellationToken token)
        {
            this.provider = provider;
            this.transformation = query;
            return Task.CompletedTask;
        }
    }
}