using System;
using System.Data;

namespace TheWheel.ETL.Provider.Mail
{
    public class FieldReference
    {
        public FieldReference(string name) { this.Name = name; }
        public FieldReference(int index) { this.Index = index; }

        public bool IsDefined => Name != null || Index.HasValue;

        public readonly string Name;
        public readonly int? Index;

        public string GetString(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetString(Index.Value);
            return reader.GetString(reader.GetOrdinal(Name));
        }

        public virtual bool GetBoolean(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetBoolean(Index.Value);
            return reader.GetBoolean(reader.GetOrdinal(Name));
        }

        public virtual byte GetByte(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetByte(Index.Value);
            return reader.GetByte(reader.GetOrdinal(Name));
        }

        public virtual long GetBytes(IDataReader reader, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            if (Index.HasValue)
                return reader.GetBytes(Index.Value, fieldOffset, buffer, bufferoffset, length);
            return reader.GetBytes(reader.GetOrdinal(Name), fieldOffset, buffer, bufferoffset, length);
        }

        public virtual char GetChar(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetChar(Index.Value);
            return reader.GetChar(reader.GetOrdinal(Name));
        }

        public virtual long GetChars(IDataReader reader, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            if (Index.HasValue)
                return reader.GetChars(Index.Value, fieldoffset, buffer, bufferoffset, length);
            return reader.GetChars(reader.GetOrdinal(Name), fieldoffset, buffer, bufferoffset, length);
        }

        public virtual IDataReader GetData(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetData(Index.Value);
            return reader.GetData(reader.GetOrdinal(Name));
        }

        public virtual string GetDataTypeName(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetDataTypeName(Index.Value);
            return reader.GetDataTypeName(reader.GetOrdinal(Name));
        }

        public virtual DateTime GetDateTime(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetDateTime(Index.Value);
            return reader.GetDateTime(reader.GetOrdinal(Name));
        }

        public virtual decimal GetDecimal(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetDecimal(Index.Value);
            return reader.GetDecimal(reader.GetOrdinal(Name));
        }

        public virtual double GetDouble(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetDouble(Index.Value);
            return reader.GetDouble(reader.GetOrdinal(Name));
        }

        public virtual Type GetFieldType(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetFieldType(Index.Value);
            return reader.GetFieldType(reader.GetOrdinal(Name));
        }

        public virtual float GetFloat(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetFloat(Index.Value);
            return reader.GetFloat(reader.GetOrdinal(Name));
        }

        public virtual Guid GetGuid(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetGuid(Index.Value);
            return reader.GetGuid(reader.GetOrdinal(Name));
        }

        public virtual short GetInt16(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetInt16(Index.Value);
            return reader.GetInt16(reader.GetOrdinal(Name));

        }

        public virtual int GetInt32(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetInt32(Index.Value);
            return reader.GetInt32(reader.GetOrdinal(Name));
        }

        public virtual long GetInt64(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetInt64(Index.Value);
            return reader.GetInt64(reader.GetOrdinal(Name));
        }

        public virtual string GetName(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetName(Index.Value);
            return Name;
        }

        public virtual int GetOrdinal(IDataReader reader)
        {
            if (Index.HasValue)
                return Index.Value;
            return reader.GetOrdinal(Name);
        }

        public virtual object GetValue(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.GetValue(Index.Value);
            return reader.GetValue(reader.GetOrdinal(Name));
        }

        public virtual bool IsDBNull(IDataReader reader)
        {
            if (Index.HasValue)
                return reader.IsDBNull(Index.Value);
            return reader.IsDBNull(reader.GetOrdinal(Name));
        }
    }

}