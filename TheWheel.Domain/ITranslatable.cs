using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface ITranslatable
    {
        string InternationalKey { get; set; }
    }
}
