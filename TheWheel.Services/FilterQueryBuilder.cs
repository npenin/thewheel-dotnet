using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using TheWheel.Domain;
using TheWheel.Lambda;

namespace TheWheel.Services
{
    public class FilterQueryBuilder<T> : FilterQueryBuilder
    {
        internal FilterQueryBuilder(IQueryable<T> query)
            : base(query, typeof(T))
        {
        }

        public new IQueryable<T> Query
        {
            get
            {
                var queryable = base.Query as IQueryable<T>;
                if (queryable == null)
                    return base.Query.Cast<T>();
                return queryable;
            }
        }
    }

    public class FilterQueryBuilder : IFilterCriteriaVisitor
    {
        protected static readonly Expression True = Expression.Constant(true, typeof(bool));
        protected static readonly Expression False = Expression.Constant(false, typeof(bool));
        protected static readonly System.Reflection.MethodInfo StartsWith;
        protected static readonly System.Reflection.MethodInfo EndsWith;
        protected static readonly System.Reflection.MethodInfo Contains;

        static FilterQueryBuilder()
        {
            StartsWith = typeof(string).GetMethod("StartsWith", new Type[] { typeof(string) });
            EndsWith = typeof(string).GetMethod("EndsWith", new Type[] { typeof(string) });
            Contains = typeof(string).GetMethod("Contains", new Type[] { typeof(string) });
        }

        public static FilterQueryBuilder Create(IQueryable query, Type itemType)
        {
            return new FilterQueryBuilder(query, itemType);
        }

        public static FilterQueryBuilder<T> Create<T>(IQueryable<T> query)
        {
            return new FilterQueryBuilder<T>(query);
        }

        public IQueryable Query { get { return query; } }

        private IQueryable query;
        private System.Linq.Expressions.ParameterExpression item;
        protected Expression expressionProperty = null, constraint = null;
        protected Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>> lambdaStack = new Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>>();
        protected Expression overallConstraint;
        protected Func<Expression, Expression, Expression> binaryDefaultOperation = ReflectionExpression.And;

        internal FilterQueryBuilder(IQueryable query, Type itemType)
        {
            this.query = query;
            item = itemType.AsParameter();
        }

        public void Visit(Filter filter)
        {
            if (filter == null)
                return;

            lambdaStack = new Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>>();

            foreach (FilterCriteria criteria in filter.FilterCriterias)
                criteria.Accept(this);

            if (overallConstraint == null)
                overallConstraint = constraint;
            else
                overallConstraint = binaryDefaultOperation(overallConstraint, constraint);

            if (overallConstraint != null)
                query = query.Where(item.Type, overallConstraint.ToLambda(item));
        }

        public void Visit(FilterCriteria criteria)
        {
            string propertyName = criteria.PropertyName;
            if (lambdaStack.Count == 0 || string.IsNullOrEmpty(propertyName) || !propertyName.StartsWith("."))
            {
                overallConstraint = binaryDefaultOperation(overallConstraint, constraint);
                expressionProperty = item;
                constraint = expressionProperty;
                lambdaStack.Clear();
            }
            else
            {
                if (query.Expression.NodeType == ExpressionType.Convert)
                    query = query.Provider.CreateQuery((Expression)((UnaryExpression)query.Expression).Operand);
                else if (query.Expression.NodeType == ExpressionType.Call)
                    query = query.Provider.CreateQuery(((MethodCallExpression)query.Expression).Object);

                while (propertyName.StartsWith(".") && propertyName != ".")
                {
                    if (propertyName != criteria.PropertyName || expressionProperty.NodeType != ExpressionType.Parameter)
                    {
                        if (expressionProperty.NodeType == ExpressionType.Parameter)
                        {
                            expressionProperty = lambdaStack.Pop().Item1.Arguments.First();
                        }
                        if (expressionProperty.NodeType != ExpressionType.MemberAccess)
                            expressionProperty = lambdaStack.Pop().Item1.Arguments.First();

                        expressionProperty = ((MemberExpression)expressionProperty).Expression;
                    }
                    propertyName = propertyName.Substring(1);
                }
            }

            FilterOperator @operator = (FilterOperator)criteria.FilterOperator;

            if (@operator != FilterOperator.Or && propertyName != null)
            {
                Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>> beforeHandlingStack = null;

                foreach (string property in propertyName.Split('.'))
                {
                    expressionProperty = expressionProperty.Property(property);
                    var propertyType = ((System.Reflection.PropertyInfo)((MemberExpression)expressionProperty).Member).PropertyType;
                    if (propertyType.IsEnumerable())
                    {
                        if (!propertyType.IsGenericType || propertyType.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                            propertyType.GetInterfaces().FirstOrDefault(t => t.IsEnumerable());
                        propertyType = propertyType.GetGenericArguments()[0];
                        var parameter = propertyType.AsParameter();
                        var func = typeof(Func<,>).MakeGenericType(parameter.Type, typeof(bool));
                        var lambda = (binaryDefaultOperation == ReflectionExpression.And ? True : False).ToLambda(parameter);
                        var call = (MethodCallExpression)expressionProperty.Any(parameter.Type, ref parameter, lambda);
                        lambdaStack.Push(new Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>(call, lambda, func, (FilterOperator)@operator));
                        constraint = call;
                        expressionProperty = parameter;
                    }
                    else
                        constraint = expressionProperty;
                }

                Type pi;
                if (expressionProperty.NodeType == ExpressionType.MemberAccess)
                    pi = ((System.Reflection.PropertyInfo)((MemberExpression)expressionProperty).Member).PropertyType;
                else
                    pi = expressionProperty.Type;
                var piType = pi;
                if (pi.IsNullable())
                    piType = pi.GetGenericArguments()[0];

                if (!string.IsNullOrEmpty(criteria.PropertyValue) && criteria.PropertyValue.Contains(";"))
                    criteria.IsMultiple = true;

                Expression rhs = null;

                if (pi.IsNullable() && !criteria.IsMultiple)
                {
                    if (string.IsNullOrEmpty(criteria.PropertyValue))
                        rhs = Expression.Constant(null, pi);
                    else
                        if (piType.IsEnum)
                        rhs = Expression.Constant(Enum.ToObject(piType, Convert.ChangeType(criteria.PropertyValue, Enum.GetUnderlyingType(piType), CultureInfo.CurrentCulture)), piType);
                    else
                        rhs = Expression.Constant(Convert.ChangeType(criteria.PropertyValue, pi, CultureInfo.CurrentCulture));
                }
                else if (criteria.IsMultiple || @operator == FilterOperator.Contains &&
                                criteria.PropertyValue.Contains(";") && pi.GetTypeCode() != TypeCode.String)
                {
                    var values = criteria.PropertyValue.Split(';');

                    if (lambdaStack.Count == 0)
                        lambdaStack.Push(null);

                    constraint = null;
                    var oldOverallConstraint = overallConstraint;
                    overallConstraint = null;

                    var oldBinaryDefaultOperation = binaryDefaultOperation;

                    beforeHandlingStack = new Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>>();

                    while (lambdaStack.Count > 0)
                        beforeHandlingStack.Push(lambdaStack.Pop());

                    lambdaStack.Push(null);



                    foreach (var sc in values.Select(v => new FilterCriteria
                    {
                        PropertyName = (criteria.PropertyName.StartsWith(".") ? string.Join("", Enumerable.Range(0, propertyName.Cast<char>().Count(c => c == '.') + 1).Select(c => ".")) : "") + propertyName,
                        PropertyValue = v,
                        FilterOperator = @operator
                    }))
                    {
                        sc.Accept(this);
                        binaryDefaultOperation = ReflectionExpression.Or;
                        overallConstraint = binaryDefaultOperation(overallConstraint, constraint);
                        constraint = null;
                    }

                    binaryDefaultOperation = oldBinaryDefaultOperation;

                    lambdaStack.Clear();

                    while (beforeHandlingStack.Count > 0)
                        lambdaStack.Push(beforeHandlingStack.Pop());



                    constraint = overallConstraint;
                    overallConstraint = oldOverallConstraint;

                    @operator = FilterOperator.Or;

                    //lambdaStack.Push(
                    //    new Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>(
                    //        (MethodCallExpression)(constraint = Expression.Call(null, Any.MakeGenericMethod(parameter.Type), valuesExpression, lambda)),
                    //        lambda,
                    //        func,
                    //        (FilterOperator)criteria.FilterOperator
                    //        )
                    //    );

                    //rhs = parameter;
                }
                else
                {
                    if (piType.IsEnum)
                        rhs = Expression.Constant(Enum.ToObject(piType, Convert.ChangeType(criteria.PropertyValue, Enum.GetUnderlyingType(piType), CultureInfo.CurrentCulture)), piType);
                    else
                        rhs = Expression.Constant(Convert.ChangeType(criteria.PropertyValue, pi, CultureInfo.CurrentCulture));
                }

                if (piType == typeof(DateTime))
                    rhs = Expression.Constant(DateTime.ParseExact(criteria.PropertyValue, "u", CultureInfo.CurrentCulture), pi);


                switch ((FilterOperator)@operator)
                {
                    case FilterOperator.Equal:
                        if (expressionProperty.Type.IsGenericType &&
                            expressionProperty.Type.GetGenericTypeDefinition() == typeof(Nullable<>) &&
                            expressionProperty.Type.GetGenericArguments()[0] == rhs.Type)
                        {
                            constraint = Expression.And(expressionProperty.Property("HasValue"),
                                Expression.Equal(expressionProperty.Property("Value"), rhs));
                        }
                        else
                        {
                            constraint = Expression.Equal(expressionProperty, rhs);

                        }

                        break;
                    case FilterOperator.Not:
                        if (expressionProperty.NodeType == ExpressionType.Parameter && criteria.PropertyValue == null && expressionProperty.Type != item.Type)
                            constraint = null;
                        else
                            constraint = Expression.NotEqual(expressionProperty, rhs);
                        break;
                    case FilterOperator.Greater:
                        constraint = Expression.GreaterThan(expressionProperty, rhs);
                        break;
                    case FilterOperator.GreaterOrEqual:
                        constraint = Expression.GreaterThanOrEqual(expressionProperty, rhs);
                        break;
                    case FilterOperator.Lower:
                        constraint = Expression.LessThan(expressionProperty, rhs);
                        break;
                    case FilterOperator.LowerOrEqual:
                        constraint = Expression.LessThanOrEqual(expressionProperty, rhs);
                        break;
                    case FilterOperator.Contains:
                    case FilterOperator.Or:
                        break;
                    case FilterOperator.StartsWith:
                        constraint = Expression.Call(expressionProperty, StartsWith, rhs);
                        break;
                    case FilterOperator.EndsWith:
                        constraint = Expression.Call(expressionProperty, EndsWith, rhs);
                        break;
                    case FilterOperator.StringContains:
                        constraint = Expression.Call(Expression.Call(expressionProperty, "ToLower", null), Contains, Expression.Call(rhs, "ToLower", null));
                        break;
                    default:
                        throw new NotSupportedException();
                }

                if (constraint == null)
                    constraint = True;

                beforeHandlingStack = new Stack<Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>>(lambdaStack.Count);

                while (lambdaStack.Count > 0)
                {
                    var kvp = lambdaStack.Pop();

                    if (kvp == null)
                    {
                        beforeHandlingStack.Push(null);
                        continue;
                    }

                    var call = kvp.Item1;
                    var lambda = kvp.Item2;
                    lambda = binaryDefaultOperation(constraint, lambda.Body).ToLambda(lambda.Parameters.ToArray());

                    constraint = call = call.Update(null, new Expression[] { call.Arguments.First(), lambda });

                    switch (kvp.Item4)
                    {
                        case FilterOperator.Equal:
                        case FilterOperator.Contains:
                        case FilterOperator.StartsWith:
                            break;
                        case FilterOperator.Not:
                            constraint = Expression.Not(call);
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    beforeHandlingStack.Push(new Tuple<MethodCallExpression, LambdaExpression, Type, FilterOperator>(call, lambda, kvp.Item3, kvp.Item4));
                }
                //Restore the stack in case we want it to be reused afterwards
                while (beforeHandlingStack.Count > 0)
                    lambdaStack.Push(beforeHandlingStack.Pop());

                //while (beforeHandlingStack.Count > 0)
                //    beforeHandlingStack.Pop();
            }
            if (criteria.FilterCriterias.Any())
            {
                var oldBinaryDefaultOperation = binaryDefaultOperation;
                if (@operator == FilterOperator.Or)
                    binaryDefaultOperation = ReflectionExpression.Or;
                else
                    binaryDefaultOperation = ReflectionExpression.And;

                var oldOverallConstraint = overallConstraint;
                overallConstraint = null;
                constraint = null;

                //
                if (lambdaStack.Count == 0)
                    lambdaStack.Push(null);
                //
                foreach (var c in criteria.FilterCriterias)
                    c.Accept(this);

                constraint = binaryDefaultOperation(overallConstraint, constraint);
                overallConstraint = oldOverallConstraint;

                binaryDefaultOperation = oldBinaryDefaultOperation;
            }
        }

        public void Visit(DateRangeFilterCriteria criteria)
        {
            Visit(new FilterCriteria() { PropertyName = criteria.PropertyName, PropertyValue = criteria.RangeStart.ToString("u"), FilterOperator = FilterOperator.GreaterOrEqual });
            Visit(new FilterCriteria() { PropertyName = criteria.PropertyName, PropertyValue = criteria.RangeEnd.ToString("u"), FilterOperator = FilterOperator.Lower });
        }

        public void Visit(ScopeFilterCriteria criteria)
        {
            throw new NotImplementedException();
        }

        public void Visit(ExpressionFilterCriteria criteria)
        {
            throw new NotImplementedException();
        }

        public void Visit(AutoFilterCriteria criteria)
        {
            throw new NotImplementedException();
        }
    }
}
