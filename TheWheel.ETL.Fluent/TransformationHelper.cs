using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.ControlFlow;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Fluent
{
    public static partial class Helper
    {
        #region With no getter
        public static async Task<ILazyReceiver> Add<T>(this Task<ILazyReceiver> reader, string name, Func<T> fieldTransformation)
        {
            return new LazyTransformer(await reader, new[] { name }, record => Add(record, name, fieldTransformation));
        }
        public static async Task<IDataProvider> Add<T>(this Task<IDataProvider> reader, string name, Func<T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Add(record, name, fieldTransformation), token);
            return transform;
        }

        public static async Task<ILazyReceiver> Add<T>(this Task<ILazyReceiver> reader, string name, Func<IDataRecord, T> fieldTransformation)
        {
            return new LazyTransformer(await reader, new[] { name }, record => Add(record, name, fieldTransformation));
        }

        public static async Task<ILazyReceiver> Transform(this Task<ILazyReceiver> receiver, string[] names, Func<IDataRecord, IDataRecord> fieldTransformation)
        {
            return new LazyTransformer(await receiver, names, fieldTransformation);
        }
        public static Task<ILazyReceiver> Truncate(this Task<ILazyReceiver> receiver, Dictionary<string, int> truncateFields)
        {
            return receiver.Transform(new string[0], (record) => new DataRecordTruncater(record, truncateFields));
        }

        public static async Task Do(this Task<IDataProvider> receiverTask, Action<IDataRecord> action, CancellationToken token)
        {
            var receiver = await receiverTask;
            var reader = await receiver.ExecuteReaderAsync(token);
            if (reader is IEnumerable<IDataRecord> enumerator)
            {
                foreach (var record in enumerator)
                    action(record);
            }
            else
                while (reader.Read())
                    action(reader);
        }

        public static async Task Do(this Task<IDataProvider> receiverTask, Func<IDataRecord, Task> action, CancellationToken token)
        {
            var receiver = await receiverTask;
            var reader = await receiver.ExecuteReaderAsync(token);
            if (reader is IEnumerable<IDataRecord> enumerator)
            {
                foreach (var record in enumerator)
                    await action(record);
            }
            else
                while (reader.Read())
                    await action(reader);
        }

        public static async Task Do(this Task<IDataProvider> receiverTask, Func<IDataRecord, CancellationToken, Task> action, CancellationToken token)
        {
            var receiver = await receiverTask;
            var reader = await receiver.ExecuteReaderAsync(token);
            if (reader is IEnumerable<IDataRecord> enumerator)
            {
                foreach (var record in enumerator)
                    await action(record, token);
            }
            else
                while (reader.Read())
                    await action(reader, token);
        }

        public static async Task<IDataProvider> Add<T>(this Task<IDataProvider> reader, string name, Func<IDataRecord, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Add(record, name, fieldTransformation), token);
            return transform;
        }
        public static async Task<IDataProvider> Transform(this Task<IDataProvider> reader, string[] names, Func<IDataRecord, IDataRecord> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider(names);
            await transform.ReceiveAsync(await reader, fieldTransformation, token);
            return transform;
        }
        public static Task<IDataProvider> Truncate(this Task<IDataProvider> reader, Dictionary<string, int> truncateFields, CancellationToken token)
        {
            return Transform(reader, new string[0], record => new DataRecordTruncater(record, truncateFields), token);
        }

        public static async Task<ILazyReceiver> Add(this Task<ILazyReceiver> reader, IDictionary<string, object> staticValues)
        {
            var keys = staticValues.Keys.ToArray();
            var values = staticValues.Values.ToArray();
            var transform = new LazyTransformer(await reader, keys, record => new TransformRecord(record, keys, (i, getValue) =>
            {
                if (record.FieldCount <= i)
                    return values[i - record.FieldCount];
                else
                    return getValue();
            }));
            return transform;
        }

        public static async Task<IDataProvider> Add(this Task<IDataProvider> reader, IDictionary<string, object> staticValues, CancellationToken token)
        {
            var transform = new TransformProvider();
            var addRecord = new DataRecord(staticValues);
            await transform.ReceiveAsync(await reader, record => TransformRecord.Add(record, addRecord), token);
            return transform;
        }

        public static async Task<IDataProvider> Add<TQueryOptions, T>(this Task<IAsyncQueryable<TQueryOptions>> reader, string name, Func<T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Add<T>(record, name, fieldTransformation), token);
            return transform;
        }

        #endregion

        #region With getter
        public static async Task<IDataProvider> Transform<T>(this Task<IDataProvider> reader, string name, Func<Func<object>, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Transform<T>(record, name, fieldTransformation), token);
            return transform;
        }

        public static async Task<IDataProvider> Transform<T>(this Task<IDataProvider> reader, int field, Func<Func<object>, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Transform<T>(record, field, fieldTransformation), token);
            return transform;
        }

        public static async Task<IDataProvider> Transform<TQueryOptions, T>(this Task<IAsyncQueryable<TQueryOptions>> reader, string name, Func<Func<object>, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Transform<T>(record, name, fieldTransformation), token);
            return transform;
        }

        public static async Task<IDataProvider> Transform<TQueryOptions, T>(this Task<IAsyncQueryable<TQueryOptions>> reader, string name, Func<IDataRecord, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Transform<T>(record, name, fieldTransformation), token);
            return transform;
        }

        public static async Task<IDataProvider> Transform<TQueryOption, T>(this Task<IAsyncQueryable<TQueryOption>> reader, int field, Func<Func<object>, T> fieldTransformation, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await reader, record => Transform<T>(record, field, fieldTransformation), token);
            return transform;
        }

        public static async Task<ILazyReceiver> Transform<T>(this Task<ILazyReceiver> reader, string name, Func<Func<object>, T> fieldTransformation)
        {
            return new LazyTransformer(await reader, new string[0], record => Transform<T>(record, name, fieldTransformation));
        }

        public static async Task<ILazyReceiver> Transform<T>(this Task<ILazyReceiver> reader, int field, Func<Func<object>, T> fieldTransformation)
        {
            return new LazyTransformer(await reader, new string[0], record => Transform<T>(record, field, fieldTransformation));
        }

        #endregion

        public static IDataRecord Add<T>(IDataRecord record, string name, Func<T> fieldTransformation)
        {
            return TransformRecord.Add(record, name, fieldTransformation());
        }


        public static IDataRecord Add<T>(IDataRecord record, string name, Func<IDataRecord, T> fieldTransformation)
        {
            return TransformRecord.Add(record, name, fieldTransformation(record));
        }


        public static TransformRecord Transform<T>(IDataRecord record, string name, Func<IDataRecord, T> fieldTransformation)
        {
            return new TransformRecord(record, (i, getValue) =>
            {
                if (record.GetOrdinal(name) == i)
                    return fieldTransformation(record);
                else
                    return getValue();
            });
        }
        public static TransformRecord Transform<T>(IDataRecord record, string name, Func<Func<object>, T> fieldTransformation)
        {
            return new TransformRecord(record, (i, getValue) =>
            {
                if (record.GetOrdinal(name) == i)
                    return fieldTransformation(getValue);
                else
                    return getValue();
            });
        }

        public static TransformRecord Transform<T>(IDataRecord record, int field, Func<Func<object>, T> fieldTransformation)
        {
            return new TransformRecord(record, (i, getValue) =>
            {
                if (field == i)
                    return fieldTransformation(getValue);
                else
                    return getValue();
            });
        }

        public static Task<IDataProvider> Rename(this Task<IDataProvider> provider, IDictionary<string, string> mapping)
        {
            return Rename(provider, mapping, CancellationToken.None);
        }

        public static async Task<IDataProvider> Rename(this Task<IDataProvider> provider, IDictionary<string, string> mapping, CancellationToken token)
        {
            var transform = new TransformProvider();
            await transform.ReceiveAsync(await provider, record => DataRecordRenamer.Rename(record, mapping), token);
            return transform;
        }

        public static async Task<ILazyReceiver> ToLazyTable<T, TKey, TReceiver>(this Task<LookupWithPresets<T, TKey>> provider, Task<TReceiver> receiver, string tableName, DbQuery updateStatement, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return await provider.WhenMatches(receiver, new DbReceiveOptions(updateStatement, mappings.Where(m => !m.SourceColumn.StartsWith("Else.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Then."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationColumn);
              }).ToArray()), token)
            .WhenNotMatches(receiver, new SqlReceiveOptions(tableName, mappings.Where(m => !m.SourceColumn.StartsWith("Then.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Else."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationColumn);
              }).ToArray()), token)
            ;
        }
        public static async Task<ILazyReceiver> ToLazyTable<T, TKey, TReceiver>(this Task<LookupWithPresets<T, TKey>> provider, Task<TReceiver> receiver, SqlReceiveOptions options, DbQuery updateStatement, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return await provider.WhenMatches(receiver, new DbReceiveOptions(updateStatement, mappings.Where(m => !m.SourceColumn.StartsWith("Else.")).Select(m =>
             {
                 if (!m.SourceColumn.StartsWith("Then."))
                     return m;
                 if (m.DestinationOrdinal != -1)
                     return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationOrdinal);
                 return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationColumn);
             }).ToArray()), token)
            .WhenNotMatches(receiver, new SqlReceiveOptions(options, mappings.Where(m => !m.SourceColumn.StartsWith("Then.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Else."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationColumn);
              }).ToArray()), token)
            ;
        }

        public static Task<ILazyReceiver> ToLazyTable<T, TKey, TReceiver>(this Task<LookupWithPresets<T, TKey>> provider, Task<TReceiver> receiver, string tableName, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return provider.ToLazyTable(receiver, tableName, DbReceiveOptions.Update(tableName, mappings.Where(m => !m.SourceColumn.StartsWith("Else.")).ToArray()).query, token, mappings);
        }

        public static Task<ILazyReceiver> ToLazyTable<T, TKey, TReceiver>(this Task<LookupWithPresets<T, TKey>> provider, Task<TReceiver> receiver, SqlReceiveOptions options, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return provider.ToLazyTable(receiver, options, DbReceiveOptions.Update(options.TableName, mappings.Where(m => !m.SourceColumn.StartsWith("Else.")).ToArray()).query, token, mappings);
        }

        public static Task ToTable<T, TKey, TReceiver>(this Task<ControlFlow.Lookup<T, TKey>> provider, Task<TReceiver> receiver, string tableName, DbQuery updateStatement, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return provider.WhenMatches(receiver, new DbReceiveOptions(updateStatement, mappings.Where(m => !m.SourceColumn.StartsWith("Else.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Then."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationColumn);
              }).ToArray()), token)
            .WhenNotMatches(receiver, new SqlReceiveOptions(tableName, mappings.Where(m => !m.SourceColumn.StartsWith("Then.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Else."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationColumn);
              }).ToArray()), token)
            .Await();
        }

        public static Task ToTable<T, TKey, TReceiver>(this Task<ControlFlow.Lookup<T, TKey>> provider, Task<TReceiver> receiver, SqlReceiveOptions options, DbQuery updateStatement, CancellationToken token)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return provider.WhenMatches(receiver, new DbReceiveOptions(updateStatement, options.Mapping.Where(m => !m.SourceColumn.StartsWith("Else.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Then."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Then.".Length), m.DestinationColumn);
              }).ToArray()), token)
            .WhenNotMatches(receiver, new SqlReceiveOptions(options, options.Mapping.Where(m => !m.SourceColumn.StartsWith("Then.")).Select(m =>
              {
                  if (!m.SourceColumn.StartsWith("Else."))
                      return m;
                  if (m.DestinationOrdinal != -1)
                      return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationOrdinal);
                  return new SqlBulkCopyColumnMapping(m.SourceColumn.Substring("Else.".Length), m.DestinationColumn);
              }).ToArray()), token)
            .Await();
        }

        public static Task ToTable<T, TKey, TReceiver>(this Task<ControlFlow.Lookup<T, TKey>> provider, Task<TReceiver> receiver, SqlReceiveOptions options, CancellationToken token)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return ToTable(provider, receiver, options, DbReceiveOptions.Update(options.TableName, options.Mapping.Where(m => !m.SourceColumn.StartsWith("Else.")).ToArray()).query, token);
        }

        public static Task ToTable<T, TKey, TReceiver>(this Task<ControlFlow.Lookup<T, TKey>> provider, Task<TReceiver> receiver, string tableName, CancellationToken token, params SqlBulkCopyColumnMapping[] mappings)
        where TReceiver : IDataReceiver<SqlReceiveOptions>, IDataReceiver<DbReceiveOptions>
        {
            return ToTable(provider, receiver, tableName, DbReceiveOptions.Update(tableName, mappings).query, token, mappings);
        }
    }
}
