using System;
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
                    Console.Write(MapType(col));
                    Console.Write(' ');
                    Console.Write(col.Name.Parts.Last());
                    Console.WriteLine(';');
                }
                Console.WriteLine("\t}");
                Console.WriteLine('}');

            }
        }

        private static string MapType(TSqlObject column)
        {
            var type = column.GetReferencedRelationshipInstances(Column.DataType).FirstOrDefault();
            if (type == null)
                throw new KeyNotFoundException("type on " + column.Name);
            switch (type.ObjectName.ToString())
            {
                case "[int]":
                    return "int";
                case "[datetime]":
                case "[datetime2]":
                    return "System.DateTime";
                case "[varchar]":
                case "[nvarchar]":
                    return "string";
                case "[bit]":
                    return "boolean";
                case "[varbinary]":
                    return "byte[]";
                case "[float]":
                    return "float";
                case "[real]":
                    return "double";
                case "[decimal]":
                    return "decimal";
                default:
                    throw new NotSupportedException(type.ObjectName.ToString());
            }
        }
    }
}
