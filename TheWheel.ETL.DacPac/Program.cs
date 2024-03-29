﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Dac.Model;

namespace TheWheel.ETL.DacPac
{
    class Program
    {
        static void Main(string[] args)
        {
            var model = new TSqlModel(args[0]);
            foreach (var sqlObject in model.GetObjects(DacQueryScopes.UserDefined, Table.TypeClass))
            {
                Console.Write("namespace ");
                for (int i = 0; i < sqlObject.Name.Parts.Count - 1; i++)
                {
                    if (i > 0)
                        Console.Write('.');
                    Console.Write(sqlObject.Name.Parts[i]);
                }
                Console.WriteLine();
                Console.WriteLine('{');
                Console.Write("\tpublic class ");
                Console.WriteLine(sqlObject.Name.Parts[sqlObject.Name.Parts.Count - 1]);
                Console.WriteLine("\t{");
                foreach (var col in sqlObject.GetReferenced(Table.Columns))
                {
                    if (col.GetMetadata<ColumnType>(Column.ColumnType) == ColumnType.ComputedColumn)
                        continue;
                    Console.Write("\t\tpublic ");
                    Console.Write(FormatType(MapType(col)));
                    Console.Write(' ');
                    Console.Write(col.Name.Parts.Last());
                    Console.WriteLine(';');
                }
                Console.WriteLine("\t}");
                Console.WriteLine('}');

            }
        }

        private static string FormatType(Type type)
        {
            if (type.IsArray)
            {
                if (type.IsGenericType)
                    return FormatType(type.GenericTypeArguments[0]) + "[]";
                else
                    return FormatType(typeof(object[]));
            }
            if (type == typeof(int))
                return "int";
            if (type == typeof(DateTime))
                return "System.DateTime";
            if (type == typeof(string))
                return "string";
            if (type == typeof(bool))
                return "bool";
            if (type == typeof(byte[]))
                return "byte[]";
            if (type == typeof(float))
                return "float";
            if (type == typeof(double))
                return "double";
            if (type == typeof(decimal))
                return "decimal";
            return type.FullName;
        }

        private static Type MapType(TSqlObject column)
        {
            var type = column.GetReferencedRelationshipInstances(Column.DataType).FirstOrDefault();
            if (type == null)
                throw new KeyNotFoundException("type on " + column.Name);
            switch (type.ObjectName.ToString())
            {
                case "[int]":
                    return typeof(int);
                case "[datetime]":
                case "[datetime2]":
                    return typeof(System.DateTime);
                case "[varchar]":
                case "[nvarchar]":
                    return typeof(string);
                case "[bit]":
                    return typeof(bool);
                case "[varbinary]":
                    return typeof(byte[]);
                case "[float]":
                    return typeof(float);
                case "[real]":
                    return typeof(double);
                case "[decimal]":
                    return typeof(decimal);
                default:
                    throw new NotSupportedException(type.ObjectName.ToString());
            }
        }
    }
}
