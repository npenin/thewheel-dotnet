using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public partial class Filter
    {
        public Filter()
        {
            FilterCriterias = new List<FilterCriteria>();
        }

        public int FK_Owner { get; set; }
        public int Id { get; set; }
        public string Name { get; set; }

        public int? FK_Source { get; set; }

        public string Scope { get; set; }

        public ICollection<FilterCriteria> FilterCriterias { get; set; }


    }

    public partial class FilterCriteria
    {
        public FilterCriteria()
        {
            FilterCriterias = new List<FilterCriteria>();
        }
        public int Id { get; set; }

        public int? ParentFilterId { get; set; }

        public string PropertyName { get; set; }

        public string PropertyValue { get; set; }

        public int FilterOperator { get; set; }

        public int? FK_ParentFilterCriteria { get; set; }
        public bool IsMultiple { get; set; }

        public Filter Filter { get; set; }

        public ICollection<FilterCriteria> FilterCriterias { get; set; }

        public FilterCriteria Parent { get; set; }
    }

    public enum FilterOperator
    {
        Equal = 0,
        Not = 1,
        Contains = 2,
        Greater = 3,
        Lower = 4,
        GreaterOrEqual = 5,
        LowerOrEqual = 6,
        Or = 7,
        StartsWith = 8,
        EndsWith,
        StringContains,
    }
}
