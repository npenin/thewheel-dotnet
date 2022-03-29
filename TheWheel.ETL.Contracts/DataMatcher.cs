using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using TheWheel.Domain;

namespace TheWheel.ETL.Contracts
{
    public class DataMatcher
    {
        private readonly int index;
        private readonly Bag<string, TreeLeaf> aliases = new Bag<string, TreeLeaf>();
        private readonly Bag<string, string> stringAliases = new Bag<string, string>();

        public readonly string Root;
        public readonly Uri rootUri;

        public readonly List<TreeLeaf> Leaves = new List<TreeLeaf>();
        private string[] fields;

        public IDictionary<string, TreeLeaf> Aliases => aliases;

        public DataMatcher(int index, string root)
        {
            this.index = index;
            this.Root = root.Substring(0, root.Length);
            this.rootUri = new Uri(root);
        }

        public bool Matches(string path)
        {
            return path == Root;
        }

        public void AddPath(string path, TypeCode typeCode)
        {
            var leaf = new TreeLeaf(new Uri(rootUri, path).ToString(), typeCode);
            Leaves.Add(leaf);
            if (fields == null)
                fields = new string[1];
            else
                Array.Resize(ref fields, fields.Length + 1);
            fields[fields.Length - 1] = leaf.Path;
            AddMapping(path, leaf, false);
        }

        public void AddMapping(string alias, TreeLeaf target, bool verify = true, bool addField = true)
        {
            if (verify && !Leaves.Contains(target))
                if (addField)
                    AddPath(target.Path, target.TypeCode);
                else
                    throw new KeyNotFoundException($"{target} was not found when trying to add alias {alias}");
            aliases.Add(alias, target);
            stringAliases.Add(alias, target.Path);
            if (alias[0] - '0' >= 0 && alias[0] - '0' < 9 && alias[0] - '0' == index)
            {
                aliases.Add(alias.Substring(2), target);
                stringAliases.Add(alias.Substring(2), target.Path);
            }
        }

        public bool AddMappingIfNotExists(string alias, TreeLeaf target, bool verify = true, bool addField = true)
        {
            if (!aliases.TryGetValue(alias, out var result))
            {
                AddMapping(alias, target, verify, addField);
                return true;
            }
            return false;
        }

        public IDataRecord NewRecord(IDictionary<string, object> currentItem)
        {
            return new DataRecord(Leaves.Select(f =>
                Convert(currentItem[f.Path], f.TypeCode)).ToArray(),
                Leaves.Select(f => f.Path).ToArray())
                .WithAliases(stringAliases);
        }

        private static object Convert(object v, TypeCode typeCode)
        {
            if (v == null)
                return null;
            switch (typeCode)
            {
                case TypeCode.Empty:
                case TypeCode.Object:
                    return v;
                case TypeCode.DBNull:
                    return DBNull.Value;
                case TypeCode.Boolean:
                    return System.Convert.ToBoolean(v);
                case TypeCode.Char:
                    return System.Convert.ToChar(v);
                case TypeCode.SByte:
                    return System.Convert.ToSByte(v);
                case TypeCode.Byte:
                    return System.Convert.ToByte(v);
                case TypeCode.Int16:
                    return System.Convert.ToInt16(v);
                case TypeCode.UInt16:
                    return System.Convert.ToUInt16(v);
                case TypeCode.Int32:
                    return System.Convert.ToInt32(v);
                case TypeCode.UInt32:
                    return System.Convert.ToUInt32(v);
                case TypeCode.Int64:
                    return System.Convert.ToInt64(v);
                case TypeCode.UInt64:
                    return System.Convert.ToUInt64(v);
                case TypeCode.Single:
                    return System.Convert.ToSingle(v);
                case TypeCode.Double:
                    return System.Convert.ToDouble(v);
                case TypeCode.Decimal:
                    return System.Convert.ToDecimal(v);
                case TypeCode.DateTime:
                    return System.Convert.ToDateTime(v);
                case TypeCode.String:
                    return System.Convert.ToString(v);
                default:
                    throw new KeyNotFoundException(typeCode.ToString());
            }
        }
    }
}
