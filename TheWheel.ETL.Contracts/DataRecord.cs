using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;

namespace TheWheel.ETL.Contracts
{

    [DebuggerDisplay("Fields = {FieldCount}")]
    [DebuggerTypeProxy(typeof(DataRecordDebuggerProxy))]
    public class DataRecord : IDataRecord
    {
        public static DataRecord Empty = new DataRecord(new object[0], new string[0]);

        protected Array data;
        private string[] columns;
        private IDictionary<string, string> aliases;

        public DataRecord(IDictionary<string, object> data)
        : this(data.Values.ToArray(), data.Keys.ToArray())
        {
        }


        public DataRecord(IDataRecord record)
        {
            this.columns = new string[record.FieldCount];
            this.data = new object[record.FieldCount];
            for (int i = 0; i < record.FieldCount; i++)
            {
                columns[i] = record.GetName(i);
                data.SetValue(record.GetValue(i), i);
            }
        }

        public DataRecord(Array values, string[] columns)
        {
            this.data = values;
            this.columns = columns;
        }
        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public int FieldCount => columns.Length;

        public bool GetBoolean(int i)
        {
            return Convert.ToBoolean(data.GetValue(i));
        }

        public byte GetByte(int i)
        {
            return Convert.ToByte(data.GetValue(i));
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return Convert.ToChar(data.GetValue(i));
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public string[] GetNames()
        {
            return columns;
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            return data.GetValue(i).GetType().ToString();
        }

        public static DataRecord FromSingle(string name, object value)
        {
            return new DataRecord(new[] { value }, new[] { name });
        }

        internal DataRecord Merge(DataRecord addRecord)
        {
            var newColumns = new string[columns.Length + addRecord.columns.Length];
            Array.Copy(columns, newColumns, columns.Length);
            Array.Copy(addRecord.columns, 0, newColumns, columns.Length, addRecord.columns.Length);

            var newdata = new object[data.Length + addRecord.data.Length];
            Array.Copy(data, newdata, data.Length);
            Array.Copy(addRecord.data, 0, newdata, data.Length, addRecord.data.Length);

            columns = newColumns;
            data = newdata;

            return this;
        }

        internal DataRecord Rename(IDictionary<string, string> mapping)
        {
            columns = columns.Select(c =>
            {
                if (mapping.TryGetValue(c, out var result))
                    return result;
                return c;
            }).ToArray();
            return this;
        }

        public DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(data.GetValue(i));
        }

        public decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(data.GetValue(i));
        }

        public double GetDouble(int i)
        {
            return Convert.ToDouble(data.GetValue(i));
        }

        public Type GetFieldType(int i)
        {
            if (IsDBNull(i))
                return typeof(DBNull);
            return data.GetValue(i).GetType();
        }

        public float GetFloat(int i)
        {
            return Convert.ToSingle(data.GetValue(i));
        }

        public Guid GetGuid(int i)
        {
            return Guid.Parse(GetString(i));

        }

        public short GetInt16(int i)
        {
            return Convert.ToInt16(data.GetValue(i));

        }

        public int GetInt32(int i)
        {
            return Convert.ToInt32(data.GetValue(i));

        }

        public long GetInt64(int i)
        {
            return Convert.ToInt64(data.GetValue(i));
        }

        public string GetName(int i)
        {
            return columns[i];
        }

        public int GetOrdinal(string name)
        {
            if (aliases != null && aliases.TryGetValue(name, out var alias))
                return Array.IndexOf(columns, alias);
            return Array.IndexOf(columns, name);
        }

        public string GetString(int i)
        {
            return Convert.ToString(data.GetValue(i));
        }

        public object GetValue(int i)
        {
            return data.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            Array.Copy(data, values, values.Length);
            return values.Length;
        }

        public bool IsDBNull(int i)
        {
            return data.GetValue(i) == null || data.GetValue(i) == DBNull.Value;
        }

        public IDataRecord WithAliases(IDictionary<string, string> aliases)
        {
            this.aliases = aliases;
            return this;
        }

        public static IDataRecord From<T>(T current)
        {
            if (current is IDataRecord dr)
                return dr;
            return new ObjectDataRecord<T>(current);
        }

        public T To<T>()
        {
            return Reflection.TypeCache.Instance.GetFactory<T>()(this);
        }
    }
}