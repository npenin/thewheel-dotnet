using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.Domain;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Fluent
{
    public static partial class Helper
    {
        public static async Task<DataProvider<Xml, TreeOptions, ITransport<Stream>>> FromXml<TTransport>(string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var provider = new DataProvider<Xml, TreeOptions, ITransport<Stream>>();
            await provider.InitializeAsync(new TTransport());
            await provider.Transport.InitializeAsync(connectionString, parameters);
            return provider;
        }
        public static async Task<DataProvider<Csv, CsvOptions, ITransport<Stream>>> FromCsv<TTransport>(string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var provider = new DataProvider<Csv, CsvOptions, ITransport<Stream>>();
            await provider.InitializeAsync(new TTransport());
            await provider.Transport.InitializeAsync(connectionString, parameters);
            return provider;
        }

        public static async Task<ILazyReceiver> Lazy<TReceiver, TReceiverOptions>(this Task<TReceiver> receiver, TReceiverOptions options)
        where TReceiver : IDataReceiver<TReceiverOptions>
        {
            return new LazyReceiver<TReceiverOptions>(await receiver, options);
        }
        
        // public static async Task<Bag<string, bool>> Cache<TProvider, TQueryOptions>(this Task<TProvider> providerTask, TQueryOptions query, CancellationToken token)
        // where TProvider : IAsyncNewQueryable<TQueryOptions>, new()
        // {
        //     var provider = await (await providerTask).QueryNewAsync(query, token);

        //     // trace.TraceInformation(cmd.CommandText);
        //     var index = new Bag<string, bool>();
        //     string[] keys = null;

        //     using (var reader = await provider.ExecuteReaderAsync(token))
        //     {
        //         if (index.Capacity < reader.RecordsAffected)
        //             index.Resize(reader.RecordsAffected);

        //         while (reader.Read())
        //         {
        //             if (keys == null)
        //             {
        //                 keys = new string[reader.FieldCount];
        //                 for (var i = 0; i < reader.FieldCount; i++)
        //                     keys[i] = reader.GetName(i);
        //             }
        //             index.Add(string.Join("/", keys.Select(k => reader.GetValue(reader.GetOrdinal(k)))), true);
        //         }
        //     }

        //     return index;
        // }

        public static Task<Bag<string, bool>> Cache(this Task<IDataProvider> provider, CancellationToken token)
        {
            return Cache(provider, -1, (record) => true, token);
        }

        public static Task<Bag<string, IDataRecord>> Cache(this Task<IDataProvider> provider, int ignoredField, CancellationToken token)
        {
            return Cache<IDataRecord>(provider, ignoredField, record=>record, token);
        }

        public static Task<Bag<string, T>> Cache<T>(this Task<IDataProvider> provider, int ignoredField, Func<IDataRecord, T> getter, CancellationToken token)
        {
            return Cache<string, T>(provider, ignoredField, (record,keys)=>string.Join("/", keys.Select(k => record.GetValue(k))), getter, token);
        }


        public static Task<Bag<string, T>> Cache<T>(this Task<IDataProvider> provider, string ignoredField, Func<IDataRecord, T> getter, CancellationToken token)
        {
            return Cache<string, T>(provider, ignoredField, (record,keys)=>string.Join("/", keys.Select(k => record.GetValue(k))), getter, token);
        }

        public static async Task<Bag<TKey, T>> Cache<TKey, T>(this Task<IDataProvider> provider, int ignoredField, Func<IDataRecord, int[], TKey> keyGetter,Func<IDataRecord, T> getter, CancellationToken token)
        {
            // trace.TraceInformation(cmd.CommandText);
            var index = new Bag<TKey, T>();
            int[] keys = null;

            using (var reader = await (await provider).ExecuteReaderAsync(token))
            {
                if (index.Capacity < reader.RecordsAffected)
                    index.Resize(reader.RecordsAffected);

                var enumerable = reader as IEnumerator<IDataRecord>;
                if (enumerable != null)
                    while (enumerable.MoveNext())
                    {
                        if (keys == null)
                        {
                            if (ignoredField == -1)
                                keys = new int[reader.FieldCount];
                            else
                                keys = new int[reader.FieldCount - 1];
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                if (i == ignoredField)
                                    continue;
                                if (i > ignoredField && ignoredField > -1)
                                    keys[i - 1] = i;
                                else
                                    keys[i] = i;
                            }
                        }
                        index.Add(keyGetter(enumerable.Current, keys), getter(enumerable.Current));
                    }
                else
                    while (reader.Read())
                    {
                        if (keys == null)
                        {
                            if (ignoredField == -1)
                                keys = new int[reader.FieldCount];
                            else
                                keys = new int[reader.FieldCount - 1];
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                if (i == ignoredField)
                                    continue;
                                if (i > ignoredField && ignoredField > -1)
                                    keys[i - 1] = i;
                                else
                                    keys[i] = i;
                            }
                        }
                        index.Add(keyGetter(reader,keys), getter(reader));
                    }
            }

            return index;
        }


        public static async Task<Bag<TKey, T>> Cache<TKey, T>(this Task<IDataProvider> provider, string ignoredField, Func<IDataRecord,int[], TKey> keyGetter, Func<IDataRecord, T> getter, CancellationToken token)
        {
            // trace.TraceInformation(cmd.CommandText);
            var index = new Bag<TKey, T>();
            int[] keys = null;

            using (var reader = await (await provider).ExecuteReaderAsync(token))
            {
                if (index.Capacity < reader.RecordsAffected)
                    index.Resize(reader.RecordsAffected);

                while (reader.Read())
                {
                    if (keys == null)
                    {
                        if (ignoredField == null)
                            keys = new int[reader.FieldCount];
                        else
                            keys = new int[reader.FieldCount - 1];
                        int ignoredFieldOrdinal = -1;
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var name = reader.GetName(i);
                            if (name == ignoredField)
                            {
                                ignoredFieldOrdinal = i;
                                continue;
                            }
                            if (i > ignoredFieldOrdinal && ignoredFieldOrdinal > -1)
                                keys[i - 1] = i;
                            else
                                keys[i] = i;
                        }
                    }
                    index.Add(keyGetter(reader,keys), getter(reader));
                }
            }
            Console.WriteLine("Index completed");
            return index;
        }

        // public static IDataReceiver<TQuery, TTransport> AsReceiver<TQuery, TTransport>(this IDataReceiver<TQuery, TTransport> receiver)
        // where TTransport : ITransport
        // {
        //     return receiver;
        // }

        // public static IDataProvider<TQuery, TTransport> AsProvider<TQuery, TTransport>(this IDataProvider<TQuery, TTransport> provider)
        // where TTransport : ITransport
        // {
        //     return provider;
        // }

        public static async Task<TProvider> Initialize<TProvider, TTransport>(this TProvider provider, string connectionString, params KeyValuePair<string, object>[] parameters)
        where TProvider : ITransportable<TTransport>
        where TTransport : ITransport, new()
        {
            await provider.Transport.InitializeAsync(connectionString, parameters);
            return provider;
        }

        // public static async Task<IDataReceiver<TQuery>> Initialize<TQuery, TTransport>(this IDataReceiver<TQuery, TTransport> provider, string connectionString, params KeyValuePair<string, object>[] parameters)
        // where TTransport : ITransport
        // {
        //     await provider.Transport.InitializeAsync(connectionString, parameters);
        //     return provider;
        // }

        public static Task<IDataProvider> Query<T, TQuery>(this Task<T> providerTask, TQuery query)
        where T : IAsyncQueryable<TQuery>
        {
            return Query<T, TQuery>(providerTask, query, CancellationToken.None);
        }
        public static async Task<IDataProvider> Query<T, TQuery>(this Task<T> providerTask, TQuery query, CancellationToken token)
        where T : IAsyncQueryable<TQuery>
        {
            var provider = await providerTask;
            await provider.QueryAsync(query, token);
            return provider;
        }
        public static Task<T> Receive<T, TReceiveOptions>(this Task<T> receiverTask, TReceiveOptions query, IDataProvider providerTask)
        where T : IDataReceiver<TReceiveOptions>
        {
            return Receive<T, TReceiveOptions>(receiverTask, query, providerTask, CancellationToken.None);
        }
        public static async Task<T> Receive<T, TReceiveOptions>(this Task<T> receiverTask, TReceiveOptions query, IDataProvider providerTask, CancellationToken token)
        where T : IDataReceiver<TReceiveOptions>
        {
            var receiver = await receiverTask;
            await receiver.ReceiveAsync(providerTask, query, token);
            return receiver;
        }
        public static async Task<IDataProvider> Serialize(this Task<IDataProvider> provider, CancellationToken token)
        {
            var passthrough = new PassthroughProvider("noop");
            await passthrough.ReceiveAsync(await provider, token);
            return passthrough;
        }


        public static async Task<IDataReceiver<TReceiveOptions>> To<TReceiveOptions>(this Task<IDataProvider> provider, Task<IDataReceiver<TReceiveOptions>> destination, TReceiveOptions options, CancellationToken token)
        {
            return await destination.Receive(options, await provider, token);

        }
        public static async Task<IDataProvider> To<TTransformer, TReceiveOptions>(this Task<IDataProvider> provider, Task<TTransformer> destination, TReceiveOptions options, CancellationToken token)
        where TTransformer : IDataReceiver<TReceiveOptions>, IDataProvider
        {
            return await destination.Receive(options, await provider, token);
        }

        // public static Task<TProvider> Query<TProvider, TQuery>(this Task<TProvider> providerTask, TQuery query)
        // where TProvider : IDataProviderByQuery<TQuery>
        // {
        //     return Query(providerTask, query);
        // }
    }
}
