using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public class MasterData : IIdentifiable, ITranslatable, INameable
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string InternationalKey { get; set; }
        public int ParentId { get; set; }
        public virtual MasterData Parent { get; set; }
        public virtual ICollection<MasterData> Children { get; set; }
        public int RootId { get; set; }
        public virtual MasterData Root { get; set; }
    }
}
