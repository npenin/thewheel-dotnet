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
}
