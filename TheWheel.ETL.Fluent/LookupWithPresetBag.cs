using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.ControlFlow;
using TheWheel.Domain;

namespace TheWheel.ETL.Fluent
{
    public class LookupWithPresets<TKey> : Lookup<DataRecord, TKey>, ILazyReceiver
    {
        public readonly LookupWithTransformOptions<TKey> Options;

        public LookupWithPresets(Task<Bag<TKey, DataRecord>> lookupProvider, Func<IDataRecord, TKey> keyGetter)
        : this(new LookupWithTransformOptions<TKey>(lookupProvider, keyGetter))
        {

        }
        public LookupWithPresets(LookupWithTransformOptions<TKey> options)
        {
            this.Options = options;
        }

        public Task ReceiveAsync(IDataProvider provider, CancellationToken token)
        {
            return this.ReceiveAsync(provider, this.Options, token);
        }
    }

    public class LookupWithPresets<T, TKey> : Lookup<T, TKey>, ILazyReceiver
    {
        public readonly LookupWithTransformOptions<T, TKey> Options;

        public LookupWithPresets(Task<Bag<TKey, T>> lookupProvider, string[] fieldNames, Func<IDataRecord, TKey> keyGetter)
        : this(new LookupWithTransformOptions<T, TKey>() { Cache = lookupProvider, CacheComparer = keyGetter, FieldNames = fieldNames })
        {

        }
        public LookupWithPresets(LookupWithTransformOptions<T, TKey> options)
        {
            this.Options = options;
        }

        public Task ReceiveAsync(IDataProvider provider, CancellationToken token)
        {
            return this.ReceiveAsync(provider, this.Options, token);
        }
    }
}