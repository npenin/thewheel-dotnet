using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;

namespace TheWheel.ETL.Contracts
{
    internal class DataRecordDebuggerProxy
    {
        private Dictionary<string, object> dict;

        public DataRecordDebuggerProxy(IDataRecord record)
        {
            this.dict = Enumerable.Range(0, record.FieldCount).ToDictionary(i => record.GetName(i), i => record.GetValue(i));
        }

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public Dictionary<string, object> Data => dict;
    }
}