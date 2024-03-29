﻿using System;
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

        public int Id { get; set; }
        public string Name { get; set; }

        public string Scope { get; set; }

        public virtual ICollection<FilterCriteria> FilterCriterias { get; set; }


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

        public object PropertyValue { get; set; }

        public FilterOperator FilterOperator { get; set; }

        public bool IsMultiple { get; set; }

        public virtual Filter Filter { get; set; }

        public virtual ICollection<FilterCriteria> FilterCriterias { get; set; }

        public virtual FilterCriteria Parent { get; set; }
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
        EndsWith = 9,
        StringContains = 10,
    }
}
