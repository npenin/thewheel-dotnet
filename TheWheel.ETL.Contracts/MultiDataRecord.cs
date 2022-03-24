using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    [DebuggerDisplay("Fields = {FieldCount}")]
    [DebuggerTypeProxy(typeof(DataRecordDebuggerProxy))]
    public class MultiDataRecord : IDataRecord
    {
        public MultiDataRecord(params IDataRecord[] records)
        {
            foreach (var record in records)
                Add(record);
        }

        List<IDataRecord> records = new List<IDataRecord>();
        List<int> recordOffsets = new List<int>();

        private int fieldCount;

        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public int FieldCount => fieldCount;

        private List<string[]> Names = new List<string[]>();

        public bool Add(IDataRecord record)
        {
            if (record.FieldCount == 0)
                return false;
            records.Add(record);
            recordOffsets.Add(fieldCount);
            Names.Add(Enumerable.Range(0, record.FieldCount).Select(i => record.GetName(i)).ToArray());
            fieldCount += record.FieldCount;
            if (Names.SelectMany(n => n).Distinct().Count() != fieldCount)
                throw new InvalidOperationException("You cannot add a record with the same field name twice");
            return true;
        }

        public bool GetBoolean(int i)
        {
            return Call(i, (record, j) => record.GetBoolean(j));

        }

        public byte GetByte(int i)
        {
            return Call(i, (record, j) => record.GetByte(j));

        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            int recordIndex = GetRecordIndexFromFieldIndex(i);
            return records[recordIndex].GetBytes(i - recordOffsets[recordIndex], fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return Call(i, (record, j) => record.GetChar(j));

        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            int recordIndex = GetRecordIndexFromFieldIndex(i);
            return records[recordIndex].GetChars(i - recordOffsets[recordIndex], fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return Call(i, (record, j) => record.GetData(j));

        }

        public string GetDataTypeName(int i)
        {
            return Call(i, (record, j) => record.GetDataTypeName(j));

        }

        public DateTime GetDateTime(int i)
        {
            return Call(i, (record, j) => record.GetDateTime(j));
            ;
        }

        public decimal GetDecimal(int i)
        {
            return Call(i, (record, j) => record.GetDecimal(j));

        }

        public double GetDouble(int i)
        {
            return Call(i, (record, j) => record.GetDouble(j));

        }

        public Type GetFieldType(int i)
        {
            return Call(i, (record, j) => record.GetFieldType(j));

        }

        public float GetFloat(int i)
        {
            return Call(i, (record, j) => record.GetFloat(j));

        }

        public Guid GetGuid(int i)
        {
            return Call(i, (record, j) => record.GetGuid(j));

        }

        public short GetInt16(int i)
        {
            return Call(i, (record, j) => record.GetInt16(j));

        }

        public int GetInt32(int i)
        {
            return Call(i, (record, j) => record.GetInt32(j));

        }

        public long GetInt64(int i)
        {
            return Call(i, (record, j) => record.GetInt64(j));

        }

        public string GetName(int i)
        {
            return Call(i, (record, j) => record.GetName(j));

        }

        public int GetOrdinal(string name)
        {
            for (int ri = 0; ri < Names.Count; ri++)
            {
                for (int i = 0; i < Names[ri].Length; i++)
                {
                    if (Names[ri][i] == name)
                        return i + recordOffsets[ri];
                }
            }
            for (int ri = 0; ri < records.Count; ri++)
            {
                if (records[ri].GetOrdinal(name) != -1)
                {
                    return recordOffsets[ri] + records[ri].GetOrdinal(name);
                }
            }
            return -1;
        }

        public string GetString(int i)
        {
            return Call(i, (record, j) => record.GetString(j));

        }

        public object GetValue(int i)
        {
            return Call(i, (record, j) => record.GetValue(j));
        }

        public int GetValues(object[] values)
        {
            int recordIndex = 0;
            object[] buffer = new object[values.Length];
            int offset = 0;
            for (recordIndex = 0; recordIndex < recordOffsets.Count && offset < values.Length; recordIndex++)
            {
                var length = records[recordIndex].GetValues(buffer);
                Array.Copy(buffer, 0, values, offset, length);
                offset += length;
            }
            return offset;
        }

        public bool IsDBNull(int i)
        {
            return Call(i, (record, j) => record.IsDBNull(j));
        }

        private T Call<T>(int i, Func<IDataRecord, int, T> f)
        {
            int recordIndex = GetRecordIndexFromFieldIndex(i);
            return f(records[recordIndex], i - recordOffsets[recordIndex]);
        }

        private int GetRecordIndexFromFieldIndex(int i)
        {
            for (int j = 1; j < recordOffsets.Count; j++)
            {
                if (recordOffsets[j] == i)
                    return j;
                if (recordOffsets[j] > i)
                    return j - 1;
            }
            return recordOffsets.Count - 1;
        }
    }
}