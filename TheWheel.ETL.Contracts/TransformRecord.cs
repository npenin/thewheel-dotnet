using System;
using System.Data;

namespace TheWheel.ETL.Contracts
{
    public class TransformRecord : DataRecordProxy
    {
        protected readonly Func<int, Func<object>, object> transform;
        protected readonly string[] fieldNames;

        public TransformRecord(IDataRecord inner, string[] fieldNames, Func<int, Func<object>, object> fieldTransformation)
        : this(inner, fieldTransformation)
        {
            this.fieldNames = fieldNames;
        }
        public TransformRecord(IDataRecord inner, Func<int, Func<object>, object> fieldTransformation)
        : base(inner)
        {
            this.fieldNames = Array.Empty<string>();
            this.transform = fieldTransformation;
        }

        public override object this[int i] => transform(i, () => base[i]);

        public override object this[string name] => transform(GetOrdinal(name), () => base[name]);


        public override int FieldCount => base.FieldCount + fieldNames.Length;

        public override string GetName(int i)
        {
            if (i >= base.FieldCount)
                return fieldNames[i - base.FieldCount];
            return base.GetName(i);
        }

        public override int GetOrdinal(string name)
        {
            var indexOfName = Array.IndexOf(fieldNames, name);
            if (indexOfName >= 0)
                return base.FieldCount + indexOfName;
            return base.GetOrdinal(name);
        }

        public override bool GetBoolean(int i)
        {
            return Convert.ToBoolean(transform(i, () => base.GetBoolean(i)));
        }

        public override byte GetByte(int i)
        {
            return Convert.ToByte(transform(i, () => base.GetByte(i)));
        }

        public override char GetChar(int i)
        {
            return Convert.ToChar(transform(i, () => base.GetChar(i)));
        }

        public override IDataReader GetData(int i)
        {
            return (IDataReader)transform(i, () => base.GetData(i));
        }

        public static IDataRecord Add(IDataRecord irecord, IDataRecord addRecord)
        {
            var record = irecord as MultiDataRecord;
            if (record != null)
            {
                record.Add(addRecord);
                return record;
            }
            else
                return new MultiDataRecord(irecord, addRecord);
        }

        public static IDataRecord Add<T>(IDataRecord record, string fieldName, T value)
        {
            return Add(record, new DataRecord(new object[] { value }, new string[] { fieldName }));
        }

        public override DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(transform(i, () => base.GetDateTime(i)));
        }

        public override decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(transform(i, () => base.GetDecimal(i)));
        }

        public override double GetDouble(int i)
        {
            return Convert.ToDouble(transform(i, () => base.GetDouble(i)));
        }

        public override Type GetFieldType(int i)
        {
            var result = transform(i, () => base.GetFieldType(i));
            return result?.GetType();
        }

        public override float GetFloat(int i)
        {
            return Convert.ToSingle(transform(i, () => base.GetFloat(i)));
        }

        public override Guid GetGuid(int i)
        {
            var result = transform(i, () => base.GetGuid(i));
            if (result != null && result.GetType() == typeof(Guid))
                return (Guid)result;
            if (Guid.TryParse(Convert.ToString(result), out var guid))
                return guid;
            return Guid.Empty;

        }

        public override short GetInt16(int i)
        {
            return Convert.ToInt16(transform(i, () => base.GetInt16(i)));
        }

        public override int GetInt32(int i)
        {
            return Convert.ToInt32(transform(i, () => base.GetInt32(i)));
        }

        public override long GetInt64(int i)
        {
            return Convert.ToInt64(transform(i, () => base.GetInt64(i)));
        }

        public override string GetString(int i)
        {
            return Convert.ToString(transform(i, () => base.GetString(i)));
        }

        public override object GetValue(int i)
        {
            return transform(i, () => base.GetValue(i));
        }

        public override int GetValues(object[] values)
        {
            for (var i = 0; i < values.Length; i++)
                values[i] = GetValue(i);

            return values.Length;
        }

        public override bool IsDBNull(int i)
        {
            return DBNull.Value == GetValue(i);
        }
    }
}