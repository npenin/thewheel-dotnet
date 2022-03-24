using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    public class DataRecordRenamer : DataRecordProxy
    {
        private IDictionary<string, string> mapping;
        private IDictionary<string, string> reverseMapping;

        internal DataRecordRenamer(IDataRecord reader, IDictionary<string, string> mapping, IDictionary<string, string> reverseMapping)
        : base(reader)
        {
            this.mapping = mapping;
            this.reverseMapping = reverseMapping;
        }

        public DataRecordRenamer(IDataRecord reader, IDictionary<string, string> mapping)
        : this(reader, mapping, new Bag<string, string>(mapping.Select(kvp => new KeyValuePair<string, string>(kvp.Value, kvp.Key))))
        {
        }

        public override object this[string name] => base[reverseMapping[name]];

        public override string GetName(int i)
        {
            return mapping[base.GetName(i)];
        }

        public override int GetOrdinal(string name)
        {
            return base.GetOrdinal(reverseMapping[name]);
        }

        internal static IDataRecord Rename(IDataRecord current, IDictionary<string, string> mapping, IDictionary<string, string> reverseMapping)
        {
            var record = current as DataRecord;
            if (record != null)
            {
                return record.Rename(mapping);
            }
            else
                return new DataRecordRenamer(current, mapping, reverseMapping);
        }
        public static IDataRecord Rename(IDataRecord current, IDictionary<string, string> mapping)
        {
            var record = current as DataRecord;
            if (record != null)
            {
                return record.Rename(mapping);
            }
            else
                return new DataRecordRenamer(current, mapping);
        }
    }
}