using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace System
{
    public static class TypeCodeExtensions
    {
        public static TypeCode GetTypeCode(this Type type)
        {
            if (type == typeof(bool) || type == typeof(Boolean))
                return TypeCode.Boolean;
            if (type == typeof(byte) || type == typeof(Byte))
                return TypeCode.Byte;
            if (type == typeof(char) || type == typeof(Char))
                return TypeCode.Char;
            if (type == typeof(DateTime))
                return TypeCode.DateTime;
            if (type == typeof(decimal) || type == typeof(Decimal))
                return TypeCode.Decimal;
            if (type == typeof(double) || type == typeof(Double))
                return TypeCode.Double;
            if (type == typeof(short) || type == typeof(Int16))
                return TypeCode.Int16;
            if (type == typeof(Int32) || type == typeof(Int32))
                return TypeCode.Int32;
            if (type == typeof(long) || type == typeof(Int64))
                return TypeCode.Int64;
            if (type == typeof(sbyte) || type == typeof(SByte))
                return TypeCode.SByte;
            if (type == typeof(float) || type == typeof(Single))
                return TypeCode.Single;
            if (type == typeof(string) || type == typeof(String))
                return TypeCode.String;
            if (type == typeof(ushort) || type == typeof(UInt16))
                return TypeCode.UInt16;
            if (type == typeof(uint) || type == typeof(UInt32))
                return TypeCode.UInt32;
            if (type == typeof(ulong) || type == typeof(UInt64))
                return TypeCode.UInt64;
            return TypeCode.Object;
        }

        public static Type ToType(this TypeCode code)
        {
            switch (code)
            {
                case TypeCode.Boolean:
                    return typeof(bool);

                case TypeCode.Byte:
                    return typeof(byte);

                case TypeCode.Char:
                    return typeof(char);

                case TypeCode.DateTime:
                    return typeof(DateTime);

                case TypeCode.Decimal:
                    return typeof(decimal);

                case TypeCode.Double:
                    return typeof(double);

                case TypeCode.Empty:
                    return null;

                case TypeCode.Int16:
                    return typeof(short);

                case TypeCode.Int32:
                    return typeof(int);

                case TypeCode.Int64:
                    return typeof(long);

                case TypeCode.Object:
                    return typeof(object);

                case TypeCode.SByte:
                    return typeof(sbyte);

                case TypeCode.Single:
                    return typeof(float);

                case TypeCode.String:
                    return typeof(string);

                case TypeCode.UInt16:
                    return typeof(ushort);

                case TypeCode.UInt32:
                    return typeof(uint);

                case TypeCode.UInt64:
                    return typeof(ulong);
            }

            return null;
        }
    }
}
