using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface IExtendable
    {
        ICollection<Property> Properties { get; set; }
        string this[string key] { get; set; }
    }

    public static partial class Extensions
    {
        public static IEnumerable<Property> Find(this IExtendable extendable, string key)
        {
            if (extendable == null)
                return Enumerable.Empty<Property>();
            else
                return extendable.Properties.Where(p => p.Name == key);
        }

        public static Property Get(this IExtendable extendable, string key)
        {
            return extendable.Find(key).FirstOrDefault();
        }
    }
}
