using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Diagnostics;
using TheWheel.Lambda;

namespace TheWheel.Domain
{
    public interface IFilterCriteriaContainer
    {
        ICollection<FilterCriteria> FilterCriterias { get; }
    }

    public partial class Filter : IFilterCriteriaContainer
    {
        public bool SkipRights { get; set; }

        private Dictionary<string, IEnumerable<FilterCriteria>> dict = new Dictionary<string, IEnumerable<FilterCriteria>>();

        public IEnumerable<FilterCriteria> Criteria<T>(Expression<Func<T, object>> constraint)
        {
            return Criteria(constraint.GetPath());
        }

        public IEnumerable<FilterCriteria> Criteria(string path)
        {
            IEnumerable<FilterCriteria> result;
            if (!dict.TryGetValue(path, out result))
            {
                result = SavedFilterCriterias.Where(c => c.PropertyName == path);

                dict.Add(path, result);
            }
            return result;
        }

        public FilterCriteria NextCriteria<T>(FilterCriteria criteria, string path)
        {
            return criteria.FilterCriterias.FirstOrDefault(c => c.PropertyName == path);
        }

        public ScopeFilterCriteria ScopeTo<T>(Expression<Func<T, object>> scope)
        {
            return ScopeTo(scope.GetPath());
        }

        public ScopeFilterCriteria ScopeTo(string scope)
        {
            ScopeFilterCriteria scopedCriteria;
            int lastMatchedIndex = -1;
            var criterias = SavedFilterCriterias.Where((c, i) =>
            {
                bool primaryMatch = c.PropertyName != null && c.PropertyName.StartsWith(scope + ".");
                if (primaryMatch || (lastMatchedIndex == i - 1 && c.PropertyName != null && c.PropertyName.StartsWith("#")))
                    lastMatchedIndex = i;
                return lastMatchedIndex == i;
            }).ToList();
            if (!criterias.Any())
            {
                scopedCriteria = SavedFilterCriterias.OfType<ScopeFilterCriteria>().FirstOrDefault(sfc => sfc.Scope == scope);
                if (scopedCriteria == null)
                    scopedCriteria = new ScopeFilterCriteria(scope, criterias);
            }
            else
            {
                foreach (var criteria in criterias)
                {
                    SavedFilterCriterias.Remove(criteria);
                }
                scopedCriteria = new ScopeFilterCriteria(scope, criterias);
            }
            SavedFilterCriterias.Add(scopedCriteria);
            return scopedCriteria;
        }

        //public static Filter CreateTemporaryFilter(Filter filter)
        //{
        //    Filter f = new Filter();
        //    f.RangeStart = filter.RangeStart;
        //    f.RangeEnd = filter.RangeEnd;
        //    foreach (var criteria in filter.FilterCriterias)
        //        f.FilterCriterias.Add(new FilterCriteria { PropertyName = criteria.PropertyName, PropertyValue = criteria.PropertyValue, FilterOperator = criteria.FilterOperator });
        //    return f;
        //}

        public bool Any(string propertyPath)
        {
            return Any(fc => fc.PropertyName == propertyPath);
        }

        public bool Any<T>(Expression<Func<T, object>> func)
        {
            return Any(func.GetPath());
        }

        public bool Any(Func<FilterCriteria, bool> func)
        {
            foreach (FilterCriteria criteria in SavedFilterCriterias)
            {
                if (func(criteria))
                    return true;
                if (criteria.FilterCriterias.Count > 0 && criteria.Any(func))
                    return true;
            }
            return false;
        }

        private ICollection<FilterCriteria> savedFilterCriterias;

        public ICollection<FilterCriteria> SavedFilterCriterias
        {
            get { return savedFilterCriterias ?? FilterCriterias; }
            set { savedFilterCriterias = value; }
        }

        public void Add(FilterCriteria filterCriteria)
        {
            SavedFilterCriterias.Add(filterCriteria);
        }

        public bool Any()
        {
            return SavedFilterCriterias.Any();
        }
    }

    public partial class FilterCriteria : IFilterCriteriaContainer
    {
        public virtual void Accept(IFilterCriteriaVisitor visitor)
        {
            visitor.Visit(this);
        }

        internal bool Any(Func<FilterCriteria, bool> func)
        {
            foreach (FilterCriteria criteria in FilterCriterias)
            {
                if (func(criteria))
                    return true;
                if (criteria.FilterCriterias.Count > 0 && criteria.Any(func))
                    return true;
            }
            return false;
        }

        public static FilterCriteria Where<T, U>(Expression<Func<T, U>> expression, U valueToCompare, FilterOperator @operator = Domain.FilterOperator.Equal)
        {
            return new FilterCriteria()
            {
                PropertyName = expression.GetPath(),
                PropertyValue = Convert.ToString(valueToCompare),
                FilterOperator = (int)@operator
            };
        }
    }

    public class AutoFilterCriteria : FilterCriteria
    {
        public AutoFilterCriteria(IEnumerable<FilterCriteria> criterias)
        {
            FilterCriterias = new List<FilterCriteria>(criterias);
        }

        public override void Accept(IFilterCriteriaVisitor visitor)
        {
            visitor.Visit(this);
        }
    }

    public class DateRangeFilterCriteria : FilterCriteria
    {
        public DateTime RangeStart { get; set; }
        public DateTime RangeEnd { get; set; }

        [DebuggerStepThrough]
        public override void Accept(IFilterCriteriaVisitor visitor)
        {
            visitor.Visit(this);
        }
    }

    public class ScopeFilterCriteria : FilterCriteria
    {
        public string Scope { get { return PropertyName; } }

        //public ICollection<FilterCriteria> FilterCriterias { get; private set; }

        public ScopeFilterCriteria(string scope, IEnumerable<FilterCriteria> enumerable)
            : this(scope, new List<FilterCriteria>(enumerable))
        {
        }

        public ScopeFilterCriteria(string scope, IList<FilterCriteria> criterias)
        {
            PropertyName = scope;
            FilterCriterias = criterias;
        }
        [DebuggerStepThrough]
        public override void Accept(IFilterCriteriaVisitor visitor)
        {
            visitor.Visit(this);
        }
    }

    public abstract class ExpressionFilterCriteria : FilterCriteria
    {
        public ExpressionFilterCriteria(LambdaExpression exp)
        {
            Expression = exp;
        }

        public LambdaExpression Expression { get; set; }

        [DebuggerStepThrough]
        public override void Accept(IFilterCriteriaVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
    public class ExpressionFilterCriteria<T> : ExpressionFilterCriteria
    {
        public ExpressionFilterCriteria(Expression<Func<T, bool>> expression)
            : base(expression)
        {
        }
    }
}
