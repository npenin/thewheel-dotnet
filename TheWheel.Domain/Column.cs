using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TheWheel.Lambda;

namespace TheWheel.Domain
{
    public class Column : IColumn
    {
        public int Id { get; set; }
        public int Order { get; set; }
        public string TranslatedName { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public object Evaluate(object target)
        {
            return target.Property(Code);
        }
    }
}
