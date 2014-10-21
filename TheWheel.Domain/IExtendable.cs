using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface IExtendable : IExtendable<Property>
    {
    }

    public interface IExtendable<T>
        where T : INameable
    {
        ICollection<T> Properties { get; set; }
        T this[string key] { get; set; }
    }

    public static partial class Extensions
    {
        public static IEnumerable<T> Find<T>(this IExtendable<T> extendable, string key)
            where T : INameable
        {
            if (extendable == null)
                return Enumerable.Empty<T>();
            else
                return extendable.Properties.Where(p => p.Name == key);
        }

        public static T Get<T>(this IExtendable<T> extendable, string key)
            where T : INameable
        {
            return extendable.Find<T>(key).FirstOrDefault();
        }
    }
}
