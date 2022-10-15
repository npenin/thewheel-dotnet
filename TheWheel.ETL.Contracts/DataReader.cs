using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace TheWheel.ETL.Contracts
{

    public abstract class DataReader : IDataReader, IEnumerator<IDataRecord>
    {
        protected readonly ILogger trace;
        private bool verbose;

        public DataReader(string traceName)
        {
            trace = TheWheel.ETL.Logging.factory.CreateLogger("TheWheel.ETL." + traceName);
            verbose = trace.IsEnabled(LogLevel.Debug);
        }

        public IDataRecord Current { get; protected set; }

        public object this[int i] => Current[i];

        public object this[string name] => Current[name];

        public abstract int Depth { get; }

        public abstract bool IsClosed { get; }

        public abstract int RecordsAffected { get; }

        public virtual int FieldCount => Current.FieldCount;

        object IEnumerator.Current => Current;

        public abstract void Close();

        public virtual void Dispose()
        {
            if (verbose)
                trace.LogDebug(0, "Disposing");
            Current = null;
        }

        public bool GetBoolean(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetBoolean) + '(' + i + ')');
            return Current.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetByte) + '(' + i + ')');
            return Current.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetBytes) + '(' + i + ')');
            return Current.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetChar) + '(' + i + ')');
            return Current.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetChars) + '(' + i + ')');
            return Current.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetData) + '(' + i + ')');
            return Current.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetDataTypeName) + '(' + i + ')');
            return Current.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetDateTime) + '(' + i + ')');
            return Current.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetDecimal) + '(' + i + ')');
            return Current.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetDouble) + '(' + i + ')');
            return Current.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetFieldType) + '(' + i + ')');
            return Current.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetFloat) + '(' + i + ')');
            return Current.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetGuid) + '(' + i + ')');
            return Current.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetInt16) + '(' + i + ')');
            return Current.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetInt32) + '(' + i + ')');

            return Current.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetInt64) + '(' + i + ')');

            return Current.GetInt64(i);
        }

        public string GetName(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetName) + '(' + i + ')');

            return Current.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetOrdinal) + '(' + name + ')');

            return Current.GetOrdinal(name);
        }

        public abstract DataTable GetSchemaTable();

        public string GetString(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetString) + '(' + i + ')');

            return Current.GetString(i);
        }

        public object GetValue(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetValue) + '(' + i + ')');

            return Current.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            if (verbose)
                trace.LogDebug(100, nameof(GetValues) + "()");

            return Current.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            if (verbose)
                trace.LogDebug(100, nameof(IsDBNull) + '(' + i + ')');

            return Current.IsDBNull(i);
        }

        public abstract bool NextResult();

        public abstract bool Read();

        public bool MoveNext()
        {
            return Read();
        }

        public virtual void Reset()
        {
            throw new NotSupportedException();
        }

        public static IDataReader From<T>(IEnumerable<T> data, CancellationToken token)
        {
            return new EnumerableDataReader<T>(data, token);
        }
        public static IDataReader Empty = new EmptyDataReader();
    }

    public sealed class EmptyDataReader : DataReader
    {
        public EmptyDataReader()
        : base("TheWheel.ETL.Empty")
        {

        }

        public override int Depth => 0;

        public override bool IsClosed => true;

        public override int RecordsAffected => 0;

        public override void Close()
        {
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            return false;
        }
    }

    public class EnumerableDataReader<T> : DataReader
    {
        protected readonly IEnumerator<T> enumerator;
        protected readonly CancellationToken token;

        public EnumerableDataReader(IEnumerable<T> source, CancellationToken token)
        : base("TheWheel.ETL.EnumerableDataReader")
        {
            this.enumerator = source.GetEnumerator();
            this.token = token;
        }

        public override int Depth => 0;

        public override bool IsClosed => enumerator.Current == null;

        public override int RecordsAffected => 0;

        public override void Close()
        {
            enumerator.Dispose();
        }

        public override void Dispose()
        {
            enumerator.Dispose();
            base.Dispose();
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            if (!enumerator.MoveNext())
                return false;
            if (token.IsCancellationRequested)
                return false;
            Current = DataRecord.From(enumerator.Current);
            return true;
        }

        public override void Reset()
        {
            enumerator.Reset();
        }
    }
}