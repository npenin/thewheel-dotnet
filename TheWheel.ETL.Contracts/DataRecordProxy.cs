using System;
using System.Data;
using System.Diagnostics;

namespace TheWheel.ETL.Contracts
{
    [DebuggerDisplay("Fields = {FieldCount}")]
    [DebuggerTypeProxy(typeof(DataRecordDebuggerProxy))]
    public class DataRecordProxy : IDataRecord
    {
        protected IDataRecord record;

        public DataRecordProxy(IDataRecord reader)
        {
            this.record = reader;
        }

        public virtual object this[int i] => record[i];

        public virtual object this[string name] => record[name];

        public virtual int FieldCount => record.FieldCount;

        public virtual bool GetBoolean(int i)
        {
            return record.GetBoolean(i);
        }

        public virtual byte GetByte(int i)
        {
            return record.GetByte(i);
        }

        public virtual long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return record.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public virtual char GetChar(int i)
        {
            return record.GetChar(i);
        }

        public virtual long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return record.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public virtual IDataReader GetData(int i)
        {
            return record.GetData(i);
        }

        public virtual string GetDataTypeName(int i)
        {
            return record.GetDataTypeName(i);
        }

        public virtual DateTime GetDateTime(int i)
        {
            return record.GetDateTime(i);
        }

        public virtual decimal GetDecimal(int i)
        {
            return record.GetDecimal(i);
        }

        public virtual double GetDouble(int i)
        {
            return record.GetDouble(i);
        }

        public virtual Type GetFieldType(int i)
        {
            return record.GetFieldType(i);
        }

        public virtual float GetFloat(int i)
        {
            return record.GetFloat(i);
        }

        public virtual Guid GetGuid(int i)
        {
            return record.GetGuid(i);
        }

        public virtual short GetInt16(int i)
        {
            return record.GetInt16(i);

        }

        public virtual int GetInt32(int i)
        {
            return record.GetInt32(i);
        }

        internal IDataRecord Merge(DataRecord addRecord)
        {
            record = TransformRecord.Add(this.record, addRecord);
            return this;
        }

        public virtual long GetInt64(int i)
        {
            return record.GetInt64(i);
        }

        public virtual string GetName(int i)
        {
            return record.GetName(i);
        }

        public virtual int GetOrdinal(string name)
        {
            return record.GetOrdinal(name);
        }

        public virtual string GetString(int i)
        {
            return record.GetString(i);
        }

        public virtual object GetValue(int i)
        {
            return record.GetValue(i);
        }

        public virtual int GetValues(object[] values)
        {
            return record.GetValues(values);
        }

        public virtual bool IsDBNull(int i)
        {
            return record.IsDBNull(i);
        }
    }
}