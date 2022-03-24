using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace TheWheel.ETL.Contracts
{
    public class PassthroughReader : DataReader
    {
        public PassthroughReader(string name)
        : base(name)
        {
            this.name = name;
        }

        private SemaphoreSlim readmutex = new SemaphoreSlim(1, 1);
        private SemaphoreSlim writemutex = new SemaphoreSlim(0, 1);
        private string name;
        private bool isCurrentActive;
        private int recordsAffected;

        public override int Depth => 0;

        public override bool IsClosed => isCurrentActive && Current == null && writemutex.CurrentCount == 1;
        public override int FieldCount => Current != null ? base.FieldCount : 0;

        public override int RecordsAffected => recordsAffected;

        public override void Close()
        {
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return Read();
        }

        public async Task Push(IDataRecord record)
        {
            await readmutex.WaitAsync();
            recordsAffected++;
            Current = record;
            // if (record == null)
            //     writemutex.Release();
            writemutex.Release();
        }

        public override bool Read()
        {
            if (IsClosed)
                return false;

            if (isCurrentActive)
                readmutex.Release();
            writemutex.Wait();
            isCurrentActive = true;
            return Current != null;
        }
    }
}