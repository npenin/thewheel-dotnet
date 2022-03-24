using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    public class DataReaderRenamer : DataReaderProxy<IDataReader>
    {

        private IDictionary<string, string> mapping;
        private IDictionary<string, string> reverseMapping;

        public DataReaderRenamer(IDataReader reader, IDictionary<string, string> mapping)
        : base(reader)
        {
            this.mapping = mapping;
            this.reverseMapping = new Bag<string, string>(mapping.Select(kvp => new KeyValuePair<string, string>(kvp.Value, kvp.Key)));
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

        public override bool MoveNext()
        {
            var result = base.MoveNext();
            if (result)
                record = DataRecordRenamer.Rename(Current, mapping, reverseMapping);
            return result;
        }
    }
}