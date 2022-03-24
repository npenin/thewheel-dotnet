using System;
using System.Diagnostics;

namespace TheWheel.ETL.Contracts
{
    [DebuggerDisplay("{Path} ({TypeCode})")]
    public class TreeLeaf
    {
        public string Path;
        public readonly TypeCode TypeCode;

        public TreeLeaf(string path)
        {
            this.Path = path;
        }

        public TreeLeaf(string path, TypeCode typecode)
        {
            this.Path = path;
            this.TypeCode = typecode;
        }

        public static implicit operator TreeLeaf(string path)
        {
            return new TreeLeaf(path, TypeCode.String);
        }

        public override bool Equals(object obj)
        {
            var other = obj as TreeLeaf;
            return other != null && other.Path == this.Path;
        }

        public override int GetHashCode()
        {
            return Path.GetHashCode();
        }
        // public static implicit operator string(TreeLeaf path)
        // {
        //     return path.Path;
        // }
    }
}