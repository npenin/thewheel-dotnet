using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;

namespace TheWheel.ETL.Contracts
{

    [DebuggerDisplay("Fields = {FieldCount}")]
    [DebuggerTypeProxy(typeof(DataRecordDebuggerProxy))]
    public class DataRecordPicker : DataRecordProxy
    {
        private int[] fieldsToPick;

        public DataRecordPicker(IDataRecord record, params int[] fieldsToPick)
        : base(record)
        {
            this.fieldsToPick = fieldsToPick;
        }

        public DataRecordPicker(IDataRecord record, params string[] fieldsToPick)
        : base(record)
        {
            this.fieldsToPick = fieldsToPick.Select(base.GetOrdinal).ToArray();
        }
        public override int FieldCount => fieldsToPick.Length;

        public override int GetOrdinal(string name)
        {
            var i = base.GetOrdinal(name);
            return Array.IndexOf(fieldsToPick, i);
        }

        public override object this[int i] => record[fieldsToPick[i]];

        public override object this[string name] => record[name];

        public override bool GetBoolean(int i)
        {
            return record.GetBoolean(fieldsToPick[i]);
        }

        public override byte GetByte(int i)
        {
            return record.GetByte(fieldsToPick[i]);
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return record.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public override char GetChar(int i)
        {
            return record.GetChar(fieldsToPick[i]);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return record.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public override IDataReader GetData(int i)
        {
            return record.GetData(fieldsToPick[i]);
        }

        public override string GetDataTypeName(int i)
        {
            return record.GetDataTypeName(fieldsToPick[i]);
        }

        public override DateTime GetDateTime(int i)
        {
            return record.GetDateTime(fieldsToPick[i]);
        }

        public override decimal GetDecimal(int i)
        {
            return record.GetDecimal(fieldsToPick[i]);
        }

        public override double GetDouble(int i)
        {
            return record.GetDouble(fieldsToPick[i]);
        }

        public override Type GetFieldType(int i)
        {
            return record.GetFieldType(fieldsToPick[i]);
        }

        public override float GetFloat(int i)
        {
            return record.GetFloat(fieldsToPick[i]);
        }

        public override Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int i)
        {
            return record.GetInt16(fieldsToPick[i]);

        }

        public override int GetInt32(int i)
        {
            return record.GetInt32(fieldsToPick[i]);
        }

        public override long GetInt64(int i)
        {
            return record.GetInt64(fieldsToPick[i]);
        }

        public override string GetName(int i)
        {
            return record.GetName(fieldsToPick[i]);
        }

        public override string GetString(int i)
        {
            return record.GetString(fieldsToPick[i]);
        }

        public override object GetValue(int i)
        {
            return record.GetValue(fieldsToPick[i]);
        }

        public override int GetValues(object[] values)
        {
            return record.GetValues(values);
        }

        public override bool IsDBNull(int i)
        {
            return record.IsDBNull(fieldsToPick[i]);
        }

    }
}