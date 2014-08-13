using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TheWheel.Domain
{
    public interface IFilterCriteriaVisitor
    {
        void Visit(Filter filter);
        void Visit(FilterCriteria criteria);
        void Visit(DateRangeFilterCriteria criteria);
        void Visit(ScopeFilterCriteria criteria);
        void Visit(ExpressionFilterCriteria criteria);
        void Visit(AutoFilterCriteria criteria);
    }
}
