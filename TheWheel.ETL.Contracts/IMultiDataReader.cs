using System.Collections.Generic;
using System.Data;

namespace TheWheel.ETL.Contracts
{
    public interface IMultiDataReader : IDataReader, IEnumerator<IDataRecord>
    {
        int OutputIndex { get; }
    }

}