using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public class LazyTransformer : ILazyReceiver
    {
        private Func<IDataRecord, IDataRecord> options;
        private ILazyReceiver receiver;
        private string[] fieldNames;

        public LazyTransformer(ILazyReceiver receiver, string[] fieldNames, Func<IDataRecord, IDataRecord> options)
        {
            this.options = options;
            this.receiver = receiver;
            this.fieldNames = fieldNames;
        }

        public async Task ReceiveAsync(IDataProvider provider, CancellationToken token)
        {
            var transformer = new TransformProvider(fieldNames);
            await transformer.ReceiveAsync(provider, options, token);
            await receiver.ReceiveAsync(transformer, token);
        }
    }
}