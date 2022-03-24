using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.ControlFlow;
using TheWheel.Domain;

namespace TheWheel.ETL.Fluent
{
    public static partial class Helper
    {
        public static Task<IfSplit> If(this Task<IDataProvider> reader, Func<IDataRecord, bool> condition, CancellationToken token)
        {
            var @if = new IfSplit();
            @if.Await(reader.ContinueWith(t => @if.ReceiveAsync(t.Result, condition, token)));
            return reader.ContinueWith(t => @if, token);
        }

        public static async Task<IfSplit> If<TThenReceiverOption>(this Task<IDataProvider> reader, Func<IDataRecord, bool> condition, Task<IDataReceiver<TThenReceiverOption>> then, TThenReceiverOption options, CancellationToken token)
        {
            var @if = new IfSplit();
            var t1 = @if.ReceiveAsync(await reader, condition, token);
            var t2 = then.Receive(options, @if.Then, token);
            await Task.WhenAll(t1, t2);
            return @if;
        }
        public static async Task<IfSplit> Then<TThenReceiverOption>(this Task<IfSplit> reader, Task<IDataReceiver<TThenReceiverOption>> then, TThenReceiverOption options, CancellationToken token)
        {
            var @if = await reader;
            await then.Receive(options, @if.Then, token);
            return @if;
        }


        public static Task<Lookup<T, TKey>> Lookup<T, TKey>(this Task<IDataProvider> reader, Task<Bag<TKey, T>> lookupProvider, Func<IDataRecord, TKey> keyGetter, CancellationToken token)
        {
            return Lookup(reader, lookupProvider, "Id", keyGetter, token);
        }
        public static Task<Lookup<T, TKey>> Lookup<T, TKey>(this Task<IDataProvider> reader, Task<Bag<TKey, T>> lookupProvider, string fieldName, Func<IDataRecord, TKey> keyGetter, CancellationToken token)
        {
            return Lookup(reader, lookupProvider, new[] { fieldName }, keyGetter, token);
        }
        public static Task<Lookup<T, TKey>> Lookup<T, TKey>(this Task<IDataProvider> reader, Task<Bag<TKey, T>> lookupProvider, string[] fieldNames, Func<IDataRecord, TKey> keyGetter, CancellationToken token)
        {
            var lookup = new Lookup<T, TKey>();
            lookup.Await(reader.ContinueWith(t => lookup.ReceiveAsync(t.Result, new LookupWithTransformOptions<T, TKey> { FieldNames = fieldNames, Cache = lookupProvider, CacheComparer = keyGetter }, token), token).Unwrap());
            return reader.ContinueWith(t => lookup, token);
        }

        public static Task<LookupWithPresets<TKey>> ToLookup<TKey>(this Task<Bag<TKey, DataRecord>> lookupProvider, Func<IDataRecord, TKey> keyGetter)
        {
            var lookup = new LookupWithPresets<TKey>(new LookupWithTransformOptions<TKey>(lookupProvider, keyGetter));
            return Task.FromResult(lookup);
        }

        public static Task<LookupWithPresets<T, TKey>> ToLookup<T, TKey>(this Task<Bag<TKey, T>> lookupProvider, Func<IDataRecord, TKey> keyGetter)
        {
            return ToLookup(lookupProvider, (string[])null, keyGetter);
        }
        public static Task<LookupWithPresets<T, TKey>> ToLookup<T, TKey>(this Task<Bag<TKey, T>> lookupProvider, string fieldName, Func<IDataRecord, TKey> keyGetter)
        {
            return ToLookup(lookupProvider, new[] { fieldName }, keyGetter);
        }
        public static Task<LookupWithPresets<T, TKey>> ToLookup<T, TKey>(this Task<Bag<TKey, T>> lookupProvider, string[] fieldNames, Func<IDataRecord, TKey> keyGetter)
        {
            var lookup = new LookupWithPresets<T, TKey>(lookupProvider, fieldNames, keyGetter);
            return Task.FromResult(lookup);
        }

        public static Task<Lookup<T, TKey>> WhenMatches<T, TKey, TReceiver, TReceiveOptions>(this Task<Lookup<T, TKey>> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            return WhenMatches<Lookup<T, TKey>, T, TKey, TReceiver, TReceiveOptions>(reader, receiver, options, token);
        }
        public static Task<Lookup<T, TKey>> WhenNotMatches<T, TKey, TReceiver, TReceiveOptions>(this Task<Lookup<T, TKey>> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            return WhenNotMatches<Lookup<T, TKey>, T, TKey, TReceiver, TReceiveOptions>(reader, receiver, options, token);
        }


        public static Task<Lookup<T, TKey>> WhenMatches<T, TKey, TLazyReceiver>(this Task<Lookup<T, TKey>> reader, Task<TLazyReceiver> receiverTask, CancellationToken token)
        where TLazyReceiver : ILazyReceiver
        {
            reader.ContinueWith(async t =>
            {
                var receiver = await receiverTask;
                t.Result.Await(receiver.ReceiveAsync(t.Result.Then, token));
            }, token);
            return reader;
        }
        public static Task<Lookup<T, TKey>> WhenNotMatches<T, TKey, TLazyReceiver>(this Task<Lookup<T, TKey>> reader, Task<TLazyReceiver> receiverTask, CancellationToken token)
        where TLazyReceiver : ILazyReceiver
        {
            reader.ContinueWith(async t =>
            {
                var receiver = await receiverTask;
                t.Result.Await(receiver.ReceiveAsync(t.Result.Else, token));
            }, token);
            return reader;
        }

        public static Task<LookupWithPresets<T, TKey>> WhenMatches<T, TKey, TReceiver, TReceiveOptions>(this Task<LookupWithPresets<T, TKey>> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            return WhenMatches<LookupWithPresets<T, TKey>, T, TKey, TReceiver, TReceiveOptions>(reader, receiver, options, token);
        }
        public static Task<LookupWithPresets<T, TKey>> WhenNotMatches<T, TKey, TReceiver, TReceiveOptions>(this Task<LookupWithPresets<T, TKey>> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            return WhenNotMatches<LookupWithPresets<T, TKey>, T, TKey, TReceiver, TReceiveOptions>(reader, receiver, options, token);
        }


        private static Task<TLookup> WhenMatches<TLookup, T, TKey, TReceiver, TReceiveOptions>(Task<TLookup> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TLookup : Lookup<T, TKey>
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            reader.ContinueWith(t => t.Result.Await(receiver.Receive(options, t.Result.Then, token)), token);
            return reader;
        }
        private static Task<TLookup> WhenNotMatches<TLookup, T, TKey, TReceiver, TReceiveOptions>(Task<TLookup> reader, Task<TReceiver> receiver, TReceiveOptions options, CancellationToken token)
        where TLookup : Lookup<T, TKey>
        where TReceiver : IDataReceiver<TReceiveOptions>
        {
            reader.ContinueWith(t => t.Result.Await(receiver.Receive(options, t.Result.Else, token)), token);
            return reader;
        }

        public static async Task Await(this Task<IfSplit> task)
        {
            await (await task).Await();
        }
        public static async Task Await<T, TKey>(this Task<Lookup<T, TKey>> task)
        {
            await (await task).Await();
        }
    }
}