using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface IIdentifiable : IIdentifiable<int>
    {
    }


    public interface IIdentifiable<T>
    {
        T Id { get; set; }
    }
}
