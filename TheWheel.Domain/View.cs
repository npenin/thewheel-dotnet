using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public class View : IIdentifiable
    {
        public int Id { get; set; }

        public ICollection<ViewItem> Items { get; set; }
    }
}
