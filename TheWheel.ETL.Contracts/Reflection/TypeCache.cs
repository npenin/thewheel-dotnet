using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace TheWheel.ETL.Contracts.Reflection
{
    public delegate T CacheFactory<T>(IDataRecord record);

    public class TypeCache
    {
        public readonly static TypeCache Instance = new TypeCache();

        IDictionary<Type, CacheFactory<object>> cache = new Dictionary<Type, CacheFactory<object>>();

        public Func<IDataRecord, T> GetFactory<T>()
        {
            if (!cache.TryGetValue(typeof(T), out var factory))
                return null;
            return (record) => (T)factory(record);
        }

        public Func<IDataRecord, T> BuildFactory<T>()
        {
            var @new = Expression.New(typeof(T));
            var record = Expression.Parameter(typeof(IDataRecord));
            return Expression.Lambda<Func<IDataRecord, T>>(Expression.MemberInit(@new, typeof(T).GetMembers(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.SetField | System.Reflection.BindingFlags.SetProperty).Select(m =>
            {
                if (m.MemberType == System.Reflection.MemberTypes.Field)
                    return Expression.Bind(m, GetExpression(record, m.Name, ((FieldInfo)m).FieldType));
                return Expression.Bind(m, GetExpression(record, m.Name, ((PropertyInfo)m).PropertyType));
            })), record).Compile();
        }

        private static Expression GetExpression(ParameterExpression record, string name, Type type)
        {
            var ordinal = Expression.Call(record, nameof(IDataRecord.GetOrdinal), null, Expression.Constant(name));
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                    return Expression.Call(record, nameof(IDataRecord.GetBoolean), null, ordinal);
                case TypeCode.Byte:
                    return Expression.Call(record, nameof(IDataRecord.GetByte), null, ordinal);
                case TypeCode.Char:
                    return Expression.Call(record, nameof(IDataRecord.GetChar), null, ordinal);
                case TypeCode.DateTime:
                    return Expression.Call(record, nameof(IDataRecord.GetDateTime), null, ordinal);
                case TypeCode.Decimal:
                    return Expression.Call(record, nameof(IDataRecord.GetDecimal), null, ordinal);
                case TypeCode.Double:
                    return Expression.Call(record, nameof(IDataRecord.GetDouble), null, ordinal);
                case TypeCode.Int16:
                    return Expression.Call(record, nameof(IDataRecord.GetInt16), null, ordinal);
                case TypeCode.Int32:
                    return Expression.Call(record, nameof(IDataRecord.GetInt32), null, ordinal);
                case TypeCode.Int64:
                    return Expression.Call(record, nameof(IDataRecord.GetInt64), null, ordinal);
                case TypeCode.SByte:
                    return Expression.Convert(Expression.Call(record, nameof(IDataRecord.GetByte), null, ordinal), typeof(sbyte));
                case TypeCode.Single:
                    return Expression.Call(record, nameof(IDataRecord.GetFloat), null, ordinal);
                case TypeCode.String:
                    return Expression.Call(record, nameof(IDataRecord.GetString), null, ordinal);
                case TypeCode.UInt16:
                    return Expression.Convert(Expression.Call(record, nameof(IDataRecord.GetInt64), null, ordinal), typeof(ushort));
                case TypeCode.UInt32:
                    return Expression.Convert(Expression.Call(record, nameof(IDataRecord.GetInt64), null, ordinal), typeof(uint));
                case TypeCode.UInt64:
                    return Expression.Convert(Expression.Call(record, nameof(IDataRecord.GetInt64), null, ordinal), typeof(ulong));
                case TypeCode.Object:
                default:
                    return Expression.Call(record, nameof(IDataRecord.GetValue), null, ordinal);

            }
        }

    }
}