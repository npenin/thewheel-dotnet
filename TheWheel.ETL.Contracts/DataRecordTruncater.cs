using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    public class DataRecordTruncater : DataRecordProxy
    {
        private IDictionary<string, int> truncateFields;
        private IDictionary<int, string> mapping;

        public DataRecordTruncater(IDataRecord reader, IDictionary<string, int> truncateFields)
        : base(reader)
        {
            this.truncateFields = truncateFields;
        }

        private string Truncate(string value, int maxLength)
        {
            if (value.Length > maxLength)
                return value.Substring(0, maxLength);
            return value;
        }

        private object Truncate(int i, object value)
        {
            if (value is string s)
                return Truncate(i, s);
            return value;
        }

        private string Truncate(int i, string s)
        {
            if (mapping == null)
                mapping = truncateFields.Keys.ToDictionary(k => GetOrdinal(k), k => k);
            if (!string.IsNullOrEmpty(s) && mapping.TryGetValue(i, out var name) && truncateFields.TryGetValue(name, out int maxLength))
                return Truncate(s, maxLength);
            return s;
        }

        public override object this[string name]
        {
            get
            {
                var value = base[name];
                if (value is string s && !string.IsNullOrEmpty(s) && truncateFields.TryGetValue(name, out int maxLength))
                    return Truncate(s, maxLength);
                return value;
            }
        }

        public override object GetValue(int i)
        {
            return Truncate(i, base.GetValue(i));
        }

        public override string GetString(int i)
        {
            return Truncate(i, base.GetString(i));
        }

    }
}