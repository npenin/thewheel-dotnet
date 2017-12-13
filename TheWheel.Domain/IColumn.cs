using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface IColumn : IIdentifiable
    {
        int Order { get; set; }

        string TranslatedName { get; set; }
        string Name { get; set; }
        
        object Evaluate(object target);
    }
}
