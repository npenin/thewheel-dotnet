
using System.Collections.Generic;

namespace TheWheel.ETL.Owin
{
    public class FilterCriteria
    {
        public string PropertyName;
        public string PropertyValue;
        public FilterOperator Operator;
        public IEnumerable<FilterCriteria> FilterCriterias;
    }
}