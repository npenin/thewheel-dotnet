using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    public class TransformReader : DataReaderProxy<IDataReader>
    {
        private Func<IDataRecord, IDataRecord> transform;
        private string[] fieldNames;
        private bool started = false;

        public TransformReader(IDataReader inner, Func<IDataRecord, IDataRecord> transform)
        : base(inner)
        {
            this.transform = transform;
        }
        public TransformReader(IDataReader inner, string[] fieldNames, Func<IDataRecord, IDataRecord> transform)
        : base(inner)
        {
            this.transform = transform;
            this.fieldNames = fieldNames;
        }

        public override int FieldCount => !started || record == null ? base.FieldCount : base.FieldCount + fieldNames.Length;

        public override string GetName(int i)
        {
            if (!started && i >= base.FieldCount)
                return fieldNames[i - base.FieldCount];
            return base.GetName(i);
        }

        public override int GetOrdinal(string name)
        {
            if (!started)
            {
                int index = Array.IndexOf(fieldNames, name);
                if (index > -1)
                    return index + base.FieldCount;
            }
            return base.GetOrdinal(name);
        }

        public override bool NextResult()
        {
            var result = base.NextResult();
            if (result)
            {
                if (!started)
                {
                    if (fieldNames.Any(n =>
                    {
                        try { if (reader.GetOrdinal(n) > -1) return true; }
                        catch (Exception)
                        {
                            return false;
                        }
                        return false;
                    }))
                        throw new InvalidOperationException("Transform cannot a field with the same name :" + string.Join(", ", fieldNames));
                }
                if (reader != null)
                {
                    if (record == reader)
                        record = new DataRecord(transform(reader));
                    else
                        record = transform(record);
                }
                else
                    record = transform(record);
            }
            else
                record = null;

            started = true;
            return result;
        }

        public override bool MoveNext()
        {
            var result = base.MoveNext();
            if (result)
            {
                if (!started)
                {
                    if (fieldNames.Any(n =>
                    {
                        try { if (reader.GetOrdinal(n) > -1) return true; }
                        catch (Exception)
                        {
                            return false;
                        }
                        return false;
                    }))
                        throw new InvalidOperationException("Transform cannot a field with the same name :" + string.Join(", ", fieldNames));
                }
                if (reader != null)
                {
                    if (record == reader)
                        record = new DataRecord(transform(reader));
                    else
                        record = transform(reader);
                }
                else
                    record = transform(record);
            }
            else
                record = null;
            started = true;
            return result;
        }
    }
}