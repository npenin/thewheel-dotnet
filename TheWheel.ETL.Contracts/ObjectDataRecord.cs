using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace TheWheel.ETL.Contracts
{
    class ObjectDataRecord<T> : IDataRecord
    {
        private static MemberInfo[] members;
        private static IDictionary<string, int> ordinalMapping;
        private T current;

        static ObjectDataRecord()
        {
            members = typeof(T).GetMembers(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                .Where(m => m.MemberType == MemberTypes.Field || m.MemberType == MemberTypes.Property).ToArray();
            ordinalMapping = members.Select((m, i) => new { m.Name, i }).ToDictionary(m => m.Name, m => m.i);
        }

        public ObjectDataRecord(T current)
        {
            this.current = current;
        }

        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public int FieldCount => members.Length;

        public bool GetBoolean(int i)
        {
            return Convert.ToBoolean(GetValue(i));
        }

        public byte GetByte(int i)
        {
            return Convert.ToByte(GetValue(i));
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return Convert.ToChar(GetValue(i));
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            return (IDataReader)GetValue(i);
        }

        public string GetDataTypeName(int i)
        {
            return GetFieldType(i).FullName;
        }

        public DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(GetValue(i));
        }

        public decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(GetValue(i));

        }

        public double GetDouble(int i)
        {
            return Convert.ToDouble(GetValue(i));

        }

        public Type GetFieldType(int i)
        {
            switch (members[i].MemberType)
            {
                case MemberTypes.Field:
                    return ((FieldInfo)members[i]).FieldType;
                case MemberTypes.Property:
                    return ((PropertyInfo)members[i]).PropertyType;
                default:
                    throw new NotSupportedException();
            }
        }

        public float GetFloat(int i)
        {
            return Convert.ToSingle(GetValue(i));
        }

        public Guid GetGuid(int i)
        {
            return new Guid(GetString(i));
        }

        public short GetInt16(int i)
        {
            return Convert.ToInt16(GetValue(i));
        }

        public int GetInt32(int i)
        {
            return Convert.ToInt32(GetValue(i));
        }

        public long GetInt64(int i)
        {
            return Convert.ToInt64(GetValue(i));
        }

        public string GetName(int i)
        {
            return members[i].Name;
        }

        public int GetOrdinal(string name)
        {
            return ordinalMapping[name];
        }

        public string GetString(int i)
        {
            return Convert.ToString(GetValue(i));
        }

        public object GetValue(int i)
        {
            switch (members[i].MemberType)
            {
                case MemberTypes.Field:
                    return ((FieldInfo)members[i]).GetValue(current);
                case MemberTypes.Property:
                    return ((PropertyInfo)members[i]).GetMethod.Invoke(current, null);
                default:
                    throw new NotSupportedException();
            }
        }

        public int GetValues(object[] values)
        {
            for (int i = 0; i < values.Length && i < members.Length; i++)
                values[i] = GetValue(i);
            return Math.Min(values.Length, members.Length);
        }

        public bool IsDBNull(int i)
        {
            var value = GetValue(i);
            return value == null || value == DBNull.Value;
        }
    }
}