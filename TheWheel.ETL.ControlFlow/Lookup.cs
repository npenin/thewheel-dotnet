using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;
using TheWheel.Domain;

namespace TheWheel.ETL.ControlFlow
{
    public class Lookup<T, TKey> : IfSplit, IDataReceiver<LookupWithTransformOptions<T, TKey>>, IDataReceiver<LookupWithTransformOptions<TKey>>
    {
        public Lookup()
        {
        }

        public async Task ReceiveAsync(IDataProvider provider, LookupOptions<T, TKey> options, CancellationToken token)
        {
            var cache = await options.Cache;
            await base.ReceiveAsync(provider, (reader) => cache.ContainsKey(options.CacheComparer(reader)), token);
        }

        public async Task ReceiveAsync(IDataProvider provider, LookupWithTransformOptions<TKey> options, CancellationToken token)
        {
            var cache = await options.Cache;
            await base.ReceiveAsync(provider, (reader) =>
            {
                if (cache.TryGetValue(options.CacheComparer(reader), out var value))
                    return TransformRecord.Add(reader, value);

                return null;
            }, token);
        }

        public async Task ReceiveAsync(IDataProvider provider, LookupWithTransformOptions<T, TKey> options, CancellationToken token)
        {
            var cache = await options.Cache;
            await base.ReceiveAsync(provider, (reader) =>
            {
                if (cache.TryGetValue(options.CacheComparer(reader), out var value))
                {
                    if (options.FieldNames == null)
                        return reader;
                    var record = value as IDataRecord;
                    if (record != null)
                        return TransformRecord.Add(reader, record);
                    var values = value as IEnumerable;
                    if (values != null)
                    {
                        var array = values as Array;
                        if (array != null)
                            return TransformRecord.Add(reader, new DataRecord(array, options.FieldNames));
                        List<object> list = new List<object>();
                        foreach (var v in values)
                            list.Add(v);
                        return TransformRecord.Add(reader, new DataRecord(list.ToArray(), options.FieldNames));
                    }

                    return TransformRecord.Add(reader, new DataRecord(new object[] { value }, options.FieldNames));
                }
                cache.Add(options.CacheComparer(reader), default(T));
                return null;
            }, token);
        }
    }

    public class LookupOptions<T, TKey>
    {
        public Task<Bag<TKey, T>> Cache;
        public Func<IDataRecord, TKey> CacheComparer;
    }
    public class LookupWithTransformOptions<T, TKey> : LookupOptions<T, TKey>
    {

        public string[] FieldNames;
    }
    public class LookupWithTransformOptions<TKey> : LookupOptions<DataRecord, TKey>
    {
        public LookupWithTransformOptions(Task<Bag<TKey, DataRecord>> cache, Func<IDataRecord, TKey> cacheComparer)
        {
            Cache = cache;
            CacheComparer = cacheComparer;
        }
        public string[] FieldNames;
    }
}
