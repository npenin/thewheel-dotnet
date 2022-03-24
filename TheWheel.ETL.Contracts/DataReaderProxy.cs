using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace TheWheel.ETL.Contracts
{
    public class DataReaderProxy<TDataReader> : DataRecordProxy, IDataReader, IEnumerator<IDataRecord>
    where TDataReader : IDataReader
    {
        protected TDataReader reader;
        protected IEnumerator<IDataRecord> enumerator;

        public DataReaderProxy(TDataReader reader)
        : base(reader)
        {
            this.reader = reader;
            this.enumerator = reader as IEnumerator<IDataRecord>;
        }

        public virtual int Depth => reader.Depth;

        public virtual bool IsClosed => reader.IsClosed;

        public virtual int RecordsAffected => reader.RecordsAffected;

        public virtual IDataRecord Current => record;

        object IEnumerator.Current => Current;

        public virtual void Close()
        {
            reader.Close();
        }

        public virtual void Dispose()
        {
            reader.Dispose();
        }

        public virtual DataTable GetSchemaTable()
        {
            return reader.GetSchemaTable();
        }

        public virtual bool MoveNext()
        {
            if (enumerator != null)
            {
                var result = enumerator.MoveNext();
                record = enumerator.Current;
                return result;
            }
            return reader.Read();
        }

        public virtual bool NextResult()
        {
            return reader.NextResult();
        }

        public bool Read()
        {
            return MoveNext();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }
    }

}