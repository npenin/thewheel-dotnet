using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Fluent
{

    public static partial class Helper
    {
        public static bool GetBoolean(this IDataRecord record, string fieldName)
        {
            return record.GetBoolean(record.GetOrdinal(fieldName));

        }
        public static byte GetByte(this IDataRecord record, string fieldName)
        {
            return record.GetByte(record.GetOrdinal(fieldName));
        }
        public static long GetBytes(this IDataRecord record, string fieldName, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return record.GetBytes(record.GetOrdinal(fieldName), fieldOffset, buffer, bufferoffset, length);
        }
        public static char GetChar(this IDataRecord record, string fieldName)
        {
            return record.GetChar(record.GetOrdinal(fieldName));
        }
        public static long GetChars(this IDataRecord record, string fieldName, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return record.GetChars(record.GetOrdinal(fieldName), fieldoffset, buffer, bufferoffset, length);
        }
        public static IDataReader GetData(this IDataRecord record, string fieldName)
        {
            return record.GetData(record.GetOrdinal(fieldName));
        }
        public static string GetDataTypeName(this IDataRecord record, string fieldName)
        {
            return record.GetDataTypeName(record.GetOrdinal(fieldName));
        }
        public static DateTime GetDateTime(this IDataRecord record, string fieldName)
        {
            return record.GetDateTime(record.GetOrdinal(fieldName));
        }
        public static decimal GetDecimal(this IDataRecord record, string fieldName)
        {
            return record.GetDecimal(record.GetOrdinal(fieldName));
        }
        public static double GetDouble(this IDataRecord record, string fieldName)
        {
            return record.GetDouble(record.GetOrdinal(fieldName));
        }
        public static Type GetFieldType(this IDataRecord record, string fieldName)
        {
            return record.GetFieldType(record.GetOrdinal(fieldName));
        }
        public static float GetFloat(this IDataRecord record, string fieldName)
        {
            return record.GetFloat(record.GetOrdinal(fieldName));
        }
        public static Guid GetGuid(this IDataRecord record, string fieldName)
        {
            return record.GetGuid(record.GetOrdinal(fieldName));
        }
        public static short GetInt16(this IDataRecord record, string fieldName)
        {
            return record.GetInt16(record.GetOrdinal(fieldName));
        }
        public static int GetInt32(this IDataRecord record, string fieldName)
        {
            return record.GetInt32(record.GetOrdinal(fieldName));
        }
        public static long GetInt64(this IDataRecord record, string fieldName)
        {
            return record.GetInt64(record.GetOrdinal(fieldName));
        }
        public static string GetName(this IDataRecord record, string fieldName)
        {
            return record.GetName(record.GetOrdinal(fieldName));
        }
        public static string GetString(this IDataRecord record, string fieldName)
        {
            return record.GetString(record.GetOrdinal(fieldName));
        }
        public static object GetValue(this IDataRecord record, string fieldName)
        {
            return record.GetValue(record.GetOrdinal(fieldName));
        }
        public static bool IsDBNull(this IDataRecord record, string fieldName)
        {
            return record.IsDBNull(record.GetOrdinal(fieldName));
        }
    }
}
