using Lambda;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.Lambda
{
    public static class ReflectionExpression
    {
        private static MethodInfo ofType = typeof(Enumerable).GetMethod("OfType");
        private static MethodInfo cast = typeof(Enumerable).GetMethod("Cast");

        public static string GetPath(this Expression exp)
        {
            return PathifyExpressionVisitor.Process(exp);
        }

        public static IEnumerable OfType(this IEnumerable set, Type type)
        {
            return (IEnumerable)ofType.MakeGenericMethod(type).Invoke(null, new[] { set });
        }

        public static IEnumerable Cast(this IEnumerable set, Type type)
        {
            return (IEnumerable)cast.MakeGenericMethod(type).Invoke(null, new[] { set });
        }

        public static IEnumerable<T> Union<T>(this IEnumerable<T> source, T item)
        {
            return source.ToArray().Union(item);
        }

        public static T[] Union<T>(this T[] source, T item)
        {
            if (source == null)
                return new T[] { item };
            var result = new T[source.Length + 1];
            source.CopyTo(result, 0);
            result[source.Length] = item;
            return result;
        }

        public static IEnumerable<T> Merge<T>(this IEnumerable<T> source1, IEnumerable<T> source2)
        {
            if (source1 == null)
                return source2;
            if (source2 == null)
                return source1;
            return source1.Union(source2);
        }

        public static LambdaExpression GetterExpression(this ParameterExpression param, string property)
        {
            return param.Property(property).ToLambda(param);
        }

        public static Expression Any(this LambdaExpression getter, ref ParameterExpression param)
        {
            return getter.Body.Any(getter.ReturnType.GetGenericArguments().First(), ref param);
        }

        public static Expression Any(this Expression getter, Type type, ref ParameterExpression param)
        {
            return Expression.Call(
                null,
                typeof(Enumerable).GetMethods().FirstOrDefault(mi => mi.Name == "Any" && mi.GetParameters().Count() == 1).MakeGenericMethod(type),
                getter);
        }

        public static Expression Any(this LambdaExpression getter, ref ParameterExpression param, LambdaExpression constraint)
        {
            return getter.Body.Any(getter.ReturnType.GetGenericArguments().First(), ref param, constraint);
        }

        public static Expression Any(this Expression getter, Type type, ref ParameterExpression param, LambdaExpression constraint)
        {
            return Expression.Call(
                null,
                typeof(Enumerable).GetMethods().FirstOrDefault(mi => mi.Name == "Any" && mi.GetParameters().Count() == 2).MakeGenericMethod(type),
                getter,
                constraint);
        }

        public static MethodInfo Where(Type type)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "Where" && mi.GetParameters().Length == 0).MakeGenericMethod(type).Invoke(null, null);
        }

        public static MethodInfo Where<T>()
        {
            return new Func<IQueryable<T>, Expression<Func<T, bool>>, IQueryable<T>>(Queryable.Where<T>).Method;
        }

        public static IQueryable Where(this IQueryable source, Type t, Expression constraint)
        {
            return source.Provider.CreateQuery(
                                    Expression.Call(null,
                                    ReflectionExpression.Where(t),
                                        source.Expression,
                                        constraint));
        }

        public static MethodInfo Select(Type tSource, Type tResult)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "Select" && mi.GetParameters().Length == 0).MakeGenericMethod(tSource, tResult).Invoke(null, null);
        }

        public static Expression SelectMany(this Expression getter, Type type, ref ParameterExpression param, LambdaExpression selector)
        {
            if (!typeof(IQueryable).IsAssignableFrom(getter.Type))
                getter = getter.AsQueryable(type);
            return Expression.Call(
                null,
                SelectMany(type.GetGenericArguments()[0], selector.ReturnType),
                getter,
                selector);
        }

        public static Expression Select(this Expression getter, Type type, ref ParameterExpression param, LambdaExpression selector)
        {
            if (!typeof(IQueryable).IsAssignableFrom(getter.Type))
                getter = getter.AsQueryable(type);
            return Expression.Call(
                null,
                Select(type.GetGenericArguments()[0], selector.ReturnType),
                getter,
                selector);
        }

        public static MethodInfo SelectMany(Type tSource, Type tResult)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "SelectMany" && mi.GetParameters().Length == 0).MakeGenericMethod(tSource, tResult).Invoke(null, null);
        }

        public static MethodInfo AsQueryable(Type type)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "AsQueryable" && mi.GetParameters().Length == 0).MakeGenericMethod(type).Invoke(null, null);
        }

        public static Expression AsQueryable(this Expression expression, Type type)
        {
            if (type.IsEnumerable())
                type = type.GetGenericArguments()[0];
            if (expression.Type != typeof(IEnumerable<>).MakeGenericType(type))
                expression = Expression.Convert(expression, typeof(IEnumerable<>).MakeGenericType(type));
            return Expression.Call(null,
                AsQueryable(type),
                expression);
        }

        public static MethodInfo AsQueryable<T>()
        {
            return new Func<IEnumerable<T>, IQueryable<T>>(Queryable.AsQueryable<T>).Method;
        }

        public static MethodInfo Select<TSource, TResult>()
        {
            return new Func<IQueryable<TSource>, Expression<Func<TSource, TResult>>, IQueryable<TResult>>(Queryable.Select<TSource, TResult>).Method;
        }

        public static MethodInfo SelectMany<TSource, TResult>()
        {
            return new Func<IQueryable<TSource>, Expression<Func<TSource, IEnumerable<TResult>>>, IQueryable<TResult>>(Queryable.SelectMany<TSource, TResult>).Method;
        }

        public static MethodInfo FirstOrDefault<T>()
        {
            return new Func<IEnumerable<T>, T>(Enumerable.FirstOrDefault<T>).Method;
        }

        public static object FirstOrDefault(this IEnumerable query, Type type)
        {
            if (query == null)
                return null;

            var enumerator = query.GetEnumerator();
            if (enumerator != null && enumerator.MoveNext())
                return enumerator.Current;

            return null;
        }

        public static IList ToArrayList(this IEnumerable source)
        {
            var result = new List<object>();
            foreach (var item in source)
                result.Add(item);
            return result;
        }

        public static IList ToList(this IQueryable source)
        {
            return (IList)ToList(source.ElementType).Invoke(null, new[] { source });
        }

        public static MethodInfo ToList(Type type)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "ToList" && mi.GetParameters().Length == 0).MakeGenericMethod(type).Invoke(null, null);
        }

        public static MethodInfo ToList<T>()
        {
            return new Func<IEnumerable<T>, IList<T>>(Enumerable.ToList).Method;
        }

        public static MethodInfo OfType<T>()
        {
            return new Func<IQueryable, IQueryable<T>>(Queryable.OfType<T>).Method;
        }

        public static IQueryable<object> Select(this IQueryable source, Type tSource, Type tResult, Expression constraint)
        {
            return source.Provider.CreateQuery<object>(
                                    Expression.Call(null,
                                    ReflectionExpression.Select(tSource, tResult),
                                        source.Expression,
                                        constraint));
        }

        public static IQueryable SelectMany(this IQueryable source, Type tSource, Type tResult, Expression selector)
        {
            return source.Provider.CreateQuery<object>(
                                    Expression.Call(null,
                                    ReflectionExpression.SelectMany(tSource, tResult),
                                        source.Expression,
                                        selector));
        }

        public static ParameterExpression AsParameter(this Type type)
        {
            if (type == null)
                return null;
            return Expression.Parameter(type);
        }

        public static ParameterExpression AsParameter(this object obj)
        {
            if (obj == null)
                return null;
            return Expression.Parameter(obj.GetType());
        }

        public static object Property(this object obj, string property)
        {
            if (obj == null)
                return null;
            if (string.IsNullOrEmpty(property))
                return obj;
            var param = obj.AsParameter();
            return param.Property(property).ToLambda(param).Compile().DynamicInvoke(obj);
        }

        public static Expression Property(this Expression expression, string property)
        {
            if (string.IsNullOrEmpty(property))
                return expression;
            var properties = property.Split('.');
            if (properties.Length == 1)
            {
                return Expression.Property(expression, property);
            }

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                if (expression.Type.IsEnumerable() && i < properties.Length)
                {
                    var param = Expression.Parameter(expression.Type.GetGenericArguments()[0]);



                    return expression.Select(expression.Type, ref param, param.Property(property.Substring(i + properties.Take(i).Select(p => p.Length).Sum())).ToLambda(param));
                }
                else
                    expression = Expression.Property(expression, prop);
            }
            return expression;
        }

        public static bool IsEnumerable(this Type type)
        {
            return type.IsCollection() || type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>) || type.IsArray;
        }

        public static bool IsCollection(this Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ICollection<>);
        }

        public static bool IsNullable(this Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static Expression ExpressionEquals(this ParameterExpression param, string property, object value, Func<Expression, Expression, Expression> finalComparison)
        {
            return (Expression)typeof(ReflectionExpression).GetMethods()
                .FirstOrDefault(mi => mi.Name == "Equals" && mi.IsGenericMethodDefinition && mi.GetParameters().First().ParameterType == typeof(ParameterExpression) && mi.GetParameters().Last().ParameterType == typeof(Func<Expression, Expression, Expression>))
                .MakeGenericMethod(param.Type)
                .Invoke(null, new object[] { param, property, value, finalComparison });
        }

        public static Expression ExpressionEquals(this ParameterExpression param, object value, Func<Expression, Expression, Expression> finalComparison)
        {
            return (Expression)typeof(ReflectionExpression).GetMethods()
                .FirstOrDefault(mi => mi.Name == "Equals" && mi.IsGenericMethodDefinition && mi.GetParameters().First().ParameterType == typeof(ParameterExpression) && mi.GetParameters().Last().ParameterType == typeof(Func<Expression, Expression, Expression>))
                .MakeGenericMethod(param.Type)
                .Invoke(null, new object[] { param, "", value, finalComparison });
        }

        public static Expression<Func<Func<U, object>, Expression>> ExpressionEquals<U>(this ParameterExpression param, string property, Func<Expression, Expression, Expression> finalComparison)
        {
            return (Expression<Func<Func<U, object>, Expression>>)typeof(ReflectionExpression).GetMethods()
                .FirstOrDefault(mi => mi.Name == "Equals" && mi.IsGenericMethodDefinition && mi.GetGenericArguments().Count() == 2 && mi.GetParameters().First().ParameterType == typeof(ParameterExpression) && mi.GetParameters().Last().ParameterType == typeof(Func<Expression, Expression, Expression>))
                .MakeGenericMethod(param.Type, typeof(U))
                .Invoke(null, new object[] { param, property, finalComparison });
        }

        public static Expression<Func<Func<U, object>, Expression>> ExpressionEquals<U>(this ParameterExpression param, Func<Expression, Expression, Expression> finalComparison)
        {
            return (Expression<Func<Func<U, object>, Expression>>)typeof(ReflectionExpression).GetMethods()
                .FirstOrDefault(mi => mi.Name == "Equals" && mi.IsGenericMethodDefinition && mi.GetGenericArguments().Count() == 2 && mi.GetParameters().First().ParameterType == typeof(ParameterExpression) && mi.GetParameters().Last().ParameterType == typeof(Func<Expression, Expression, Expression>))
                .MakeGenericMethod(param.Type, typeof(U))
                .Invoke(null, new object[] { param, "", finalComparison });
        }

        public static Expression Equals<T>(this ParameterExpression param, string property, object value, Func<Expression, Expression, Expression> finalComparison)
        {
            Type t = param.GetType(property);
            Expression expression = null;
            if (!string.IsNullOrEmpty(property))
            {
                var properties = property.Split('.');
                //if (properties.Length == 1)
                //    return null;

                string currentProperty = string.Empty;
                expression = param.IsNotNullUntil(property);
                for (int i = 0; i < properties.Length - 1; i++)
                {
                    if (i > 0)
                        currentProperty += '.';
                    currentProperty += properties[i];
                    var propType = param.GetType(currentProperty);
                    if (propType.IsEnumerable())
                    {
                        var reRooted = Chroot(property, currentProperty);
                        var newParam = Expression.Parameter(propType.GetGenericArguments()[0]);
                        return expression.And(Any(ref param, currentProperty, newParam.ExpressionEquals(reRooted, value, finalComparison).ToLambda(newParam)));
                    }
                }
            }
            if (t.IsEnumerable())
            {
                var newParam = Expression.Parameter(t.GetGenericArguments()[0]);
                return expression.And(Any(ref param, property, newParam.ExpressionEquals(value, finalComparison).ToLambda(newParam)));
            }
            if (typeof(IEnumerable<>).MakeGenericType(t).IsInstanceOfType(value))
            {
                var newParam = Expression.Parameter(t);
                return expression.And(Expression.Constant(value).Any(t, ref newParam, finalComparison(param.Property(property), newParam.Convert(typeof(string), t)).ToLambda(newParam)));
                //return expression.And(finalComparison(param.Property(property), newParam.Convert(typeof(string), t).PartialEvalAs(t)).ToLambda(newParam).Any(ref newParam));
            }
            return expression.And(finalComparison(param.Property(property), Expression.Constant(value).Convert(typeof(string), t).PartialEvalAs(t)));
        }

        public static Expression<Func<object, Expression>> Equals<T>(this ParameterExpression param, string property, Func<Expression, Expression, Expression> finalComparison)
        {
            Type t = param.GetType(property);
            Expression expression = null;
            var metaParam = Expression.Parameter(typeof(object));
            if (!string.IsNullOrEmpty(property))
            {
                var properties = property.Split('.');
                //if (properties.Length == 1)
                //    return null;

                string currentProperty = string.Empty;
                expression = param.IsNotNullUntil(property);
                for (int i = 0; i < properties.Length - 1; i++)
                {
                    if (i > 0)
                        currentProperty += '.';
                    currentProperty += properties[i];
                    var propType = param.GetType(currentProperty);
                    if (propType.IsEnumerable())
                    {
                        var reRooted = Chroot(property, currentProperty);
                        var newParam = Expression.Parameter(propType.GetGenericArguments()[0]);
                        var equals = ParameterReplacerVisitor.Process(newParam.Equals<T>(reRooted, finalComparison), metaParam).Compile();
                        return ParameterReplacerVisitor.Process<Func<object, Expression>>((object obj) => expression.And(Any(ref param, currentProperty, equals(obj).ToLambda(newParam))), metaParam);
                        //return (Expression<Func<object, Expression>>)ParameterReplacerVisitor.Process<Func<object, Expression>>(obj => .ToLambda(newParam)), metaParam);

                    }
                }
            }
            if (t.IsEnumerable())
            {
                var newParam = Expression.Parameter(t.GetGenericArguments()[0]);
                return obj => expression.And(Any(ref param, property, (LambdaExpression)ParameterReplacerVisitor.Process(newParam.Equals<T>("", finalComparison), metaParam)).ToLambda(newParam));
            }
            return (object obj) => expression.And(finalComparison(param.Property(property), Expression.Constant(obj).Convert(typeof(string), t).PartialEvalAs(t)));
        }

        public static string Chroot(string property, string newRoot)
        {
            if (!property.StartsWith(newRoot))
                throw new ArgumentException("The property " + property + " may not be chrooted to " + newRoot);

            if (property == newRoot)
                return property;

            return property.Substring(newRoot.Length + 1);
        }

        public static bool IsNotNullUntil(this object param, string property)
        {
            var paramExpression = param.AsParameter();
            var exp = paramExpression.IsNotNullUntil(property);
            return exp == null && param != null || exp != null && (bool)exp.ToLambda(paramExpression).Compile().DynamicInvoke(param);
        }

        public static Expression IsNotNullUntil(this ParameterExpression param, string property)
        {
            var properties = property.Split('.');
            if (properties.Length == 1)
                return null;

            string currentProperty = string.Empty;
            var NULL = Expression.Constant(null);
            Expression expression = Expression.NotEqual(param, NULL);
            for (int i = 0; i < properties.Length - 1; i++)
            {
                if (i > 0)
                    currentProperty += '.';
                currentProperty += properties[i];
                if (expression.NodeType == ExpressionType.Lambda)
                    expression = ((LambdaExpression)expression).Body;
                var propType = param.GetType(currentProperty);
                if (propType.IsEnumerable())
                    expression = expression.And(Any(ref param, currentProperty));
                else
                    expression = expression.And(Expression.NotEqual(param.GetterExpression(currentProperty).Body, NULL));
            }
            return expression;
        }

        private static Expression Any(ref ParameterExpression param, string currentProperty)
        {
            return param.GetterExpression(currentProperty).Any(ref param);
        }

        private static Expression Any(ref ParameterExpression param, string currentProperty, LambdaExpression constraint)
        {
            return param.GetterExpression(currentProperty).Any(ref param, constraint);
        }

        public static Type GetType(this ParameterExpression param, string property)
        {
            if (string.IsNullOrEmpty(property))
                return param.Type;
            Expression expression = param;
            var properties = property.Split('.');

            if (properties.Length == 1)
                return expression.Property(property).Type;

            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                expression = expression.Property(prop);
                if (i < properties.Length - 1 && expression.Type.IsEnumerable())
                    expression = Expression.Parameter(expression.Type.GetGenericArguments()[0]);
            }
            return expression.Type;
        }

        public static Expression And(this Expression left, Expression right)
        {
            if (left == null)
                return right;
            if (right == null)
                return left;
            return Expression.AndAlso(left, right);
        }

        public static Expression<Func<T, U>> And<T, U>(this Expression<Func<T, U>> left, Expression<Func<T, U>> right)
        {
            if (left == null)
                return right;
            if (right == null)
                return left;

            var param = Expression.Parameter(typeof(T));
            var processedFilter = ParameterReplacerVisitor.Process(left, param).Body.And(ParameterReplacerVisitor.Process(right, param).Body);

            var result = processedFilter.ToLambda<Func<T, U>>(param);
            return result;
        }

        public static Expression<Func<T, U>> Or<T, U>(this Expression<Func<T, U>> left, Expression<Func<T, U>> right)
        {
            if (left == null)
                return right;
            if (right == null)
                return left;

            var param = Expression.Parameter(typeof(T));
            var processedFilter = ParameterReplacerVisitor.Process(left, param).Body.Or(ParameterReplacerVisitor.Process(right, param).Body);

            var result = processedFilter.ToLambda<Func<T, U>>(param);
            return result;
        }

        public static Expression Or(this Expression left, Expression right)
        {
            if (left == null)
                return right;
            if (right == null)
                return left;
            return Expression.OrElse(left, right);
        }

        public static Expression Convert(this Expression value, Type sourceType, Type targetType)
        {
            //if (value.Type.IsEnum)
            sourceType = value.Type;

            if (sourceType == targetType)
                return value;

            var coreType = targetType;
            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                coreType = targetType.GetGenericArguments()[0];
                //value = Expression.Condition(value.Property("HasValue"), value.Property("Value"), Expression.Constant(Activator.CreateInstance(coreType)));
            }
            var typeCode = Type.GetTypeCode(coreType);
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.DateTime:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.String:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    if (sourceType.IsValueType)
                    {
                        if (typeCode == TypeCode.String)
                            return Expression.Call(value, "ToString", null);
                        if (sourceType.IsEnum)
                            return Expression.Convert(Expression.Call(typeof(Convert).GetMethod("To" + typeCode, new Type[] { sourceType }), Expression.Convert(value, typeof(object))), targetType);
                        return Expression.Convert(Expression.Call(typeof(Convert).GetMethod("To" + typeCode, new Type[] { sourceType }), Expression.Convert(value, typeof(object))), targetType);
                    }
                    if (typeCode == TypeCode.String && value.NodeType == ExpressionType.Constant && ((ConstantExpression)value).Value == null)
                        return value;
                    return Expression.Convert(Expression.Call(typeof(Convert).GetMethod("To" + typeCode, new Type[] { sourceType }), Expression.TypeAs(value, sourceType)), targetType);
                default:
                    if (sourceType == typeof(object))
                        return Expression.Convert(value, targetType);
                    return Expression.Convert(Expression.Call(null, new Func<object, Type, IFormatProvider, object>(System.Convert.ChangeType).Method, Expression.Convert(value, typeof(object)), Expression.Constant(targetType, typeof(Type)), Expression.Constant(null)), targetType);
            }
        }

        public static Expression PartialEvalAs(this Expression expression, Type asType)
        {
            return Expression.Convert(expression.PartialEval(), asType);
        }

        public static Expression PartialEval(this Expression expression)
        {
            if (expression.Type.IsGenericType && expression.Type.GetGenericTypeDefinition() == typeof(PrecompilableExpression<>))
                expression = ReflectionExpression.CallOn(expression.Type.GetMethod("AsExpression", BindingFlags.Instance | BindingFlags.Public), expression);
            if (typeof(Expression).IsAssignableFrom(expression.Type))
                return (Expression)expression.ToLambda().Compile().DynamicInvoke();
            return Evaluator.PartialEval(expression);
        }

        public static MethodInfo Lambda(Type type)
        {
            return (MethodInfo)typeof(ReflectionExpression).GetMethods(BindingFlags.Static | BindingFlags.Public).Single(mi => mi.Name == "Lambda" && mi.GetParameters().Length == 0).MakeGenericMethod(type).Invoke(null, null);
        }

        public static MethodInfo Lambda<T>()
        {
            return new Func<Expression, ParameterExpression[], Expression<T>>(ToLambda<T>).Method;
        }

        public static LambdaExpression ToLambda(this Expression body, params ParameterExpression[] parameters)
        {
            Type funcType;
            if (parameters == null)
                funcType = body.Type.AsFuncResult();
            else
                funcType = body.Type.AsFuncResult(parameters.Select(p => p.Type));
            return (LambdaExpression)Lambda(funcType).Invoke(null, new object[] { body, parameters });
        }

        public static Expression<T> ToLambda<T>(this Expression body, params ParameterExpression[] parameters)
        {
            return Expression.Lambda<T>(body, parameters);
        }

        public static MethodInfo ToMethod<TResult>(this Func<TResult> del)
        {
            return del.Method;
        }
        public static MethodInfo ToMethod<T1, TResult>(this Func<T1, TResult> del)
        {
            return del.Method;
        }
        public static MethodInfo ToMethod<T1, T2, TResult>(this Func<T1, T2, TResult> del)
        {
            return del.Method;
        }
        public static MethodInfo ToMethod<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> del)
        {
            return del.Method;
        }

        public static Expression Call(this MethodInfo mi, params Expression[] expressions)
        {
            return Expression.Call(null, mi, expressions);
        }

        public static Expression CallOn(this MethodInfo mi, Expression instance, params Expression[] expressions)
        {
            return Expression.Call(instance, mi, expressions);
        }

        public static Expression Call<TResult>(this Func<TResult> del, params Expression[] expressions)
        {
            return Expression.Call(null, del.Method, expressions);
        }

        public static Expression Call<T1, TResult>(this Func<T1, TResult> del, params Expression[] expressions)
        {
            return Expression.Call(null, del.Method, expressions);
        }

        public static Expression Call<T1, T2, TResult>(this Func<T1, T2, TResult> del, params Expression[] expressions)
        {
            return Expression.Call(null, del.Method, expressions);
        }

        public static U AsCompilable<T, U>(this Expression<Func<T, U>> lambda)
        {
            return default(U);
        }

        public static Expression<Func<T, U>> AsLambda<T, U>(this T source, Expression<Func<T, U>> lambda)
        {
            return lambda;
        }


        public static PrecompilableExpression<Func<T, U>> AsLambda<T, U>(this T source, PrecompilableExpression<Func<T, U>> lambda)
        {
            return lambda;
        }

        public static PrecompilableExpression<Func<T, U>> PreCompile<T, U>(Expression<Func<T, U>> lambda)
        {
            return lambda;
        }

        public static U AsCompilable<T, U>(this PrecompilableExpression<Func<T, U>> lambda, T source)
        {
            return lambda.AsExpression().Compile()(source);
        }


        //public static U AsCompilable<T, U, V>(this PrecompilableExpression<Func<T, U>> lambda, V source)
        //{
        //    return default(U);
        //}

        public static Expression<T> Replace<T>(this Expression<T> lambda, ParameterExpression newParam)
        {
            return (Expression<T>)ParameterReplacerVisitor.Process(lambda, newParam);
        }

        public static LambdaExpression Replace(this LambdaExpression lambda, ParameterExpression newParam)
        {
            return (LambdaExpression)ParameterReplacerVisitor.Process(lambda, newParam);
        }

        public static LambdaExpression Replace(this LambdaExpression lambda, ParameterExpression newParam, Expression newExp)
        {
            return (LambdaExpression)ParameterReplacerVisitor.Process(lambda, newParam, newExp);
        }

        public static Expression<Func<TSource, TDestination>> Combine<TSource, TDestination>(this Expression<Func<TSource, TDestination>> init, params Expression<Func<TSource, TDestination>>[] selectors)
        {
            return Combine(selectors.Union(init));
        }

        public static Expression<Func<TSource, TDestination>> Combine<TSource, TDestination>(params Expression<Func<TSource, TDestination>>[] selectors)
        {
            var param = Expression.Parameter(typeof(TSource), "x");
            return
                Expression.MemberInit(
                    Expression.New(typeof(TDestination).GetConstructor(new Type[0])),
                    selectors.SelectMany(
                        selector => ((MemberInitExpression)selector.Body).Bindings.OfType<MemberAssignment>(),
                        (selector, binding) => Expression.Bind(binding.Member, ParameterReplacerVisitor.Process(binding.Expression, selector.Parameters[0], param))))
                .ToLambda<Func<TSource, TDestination>>(param);
        }

        public static Expression<Func<TDestination, TDestination>> Restrict<TSource, TDestination>(this Expression<Func<TSource, TDestination>> init, params Expression<Func<TSource, TDestination>>[] selectors)
        {
            var param = Expression.Parameter(typeof(TDestination), "x");
            return
                Expression.MemberInit(
                    Expression.New(typeof(TDestination).GetConstructor(new Type[0])),
                    selectors.SelectMany(
                        selector => ((MemberInitExpression)selector.Body).Bindings.OfType<MemberAssignment>(),
                        (selector, binding) => Expression.Bind(binding.Member, param.Property(binding.Member.Name))))
                .ToLambda<Func<TDestination, TDestination>>(param);
        }

        public static Type AsFuncResult(this Type resultType, IEnumerable<Type> typeParameters)
        {
            return resultType.AsFuncResult(typeParameters.ToArray());
        }

        public static Type AsFuncResult(this Type resultType, params Type[] typeParameters)
        {
            var types = typeParameters.Union(resultType);
            switch (types.Length)
            {
                case 1:
                    if (resultType == typeof(void))
                        return typeof(Action);
                    return typeof(Func<>).MakeGenericType(types);
                case 2:
                    if (resultType == typeof(void))
                        return typeof(Action<>).MakeGenericType(typeParameters);
                    return typeof(Func<,>).MakeGenericType(types);
                case 3:
                    if (resultType == typeof(void))
                        return typeof(Action<,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,>).MakeGenericType(types);
                case 4:
                    if (resultType == typeof(void))
                        return typeof(Action<,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,>).MakeGenericType(types);
                case 5:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,>).MakeGenericType(types);
                case 6:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,>).MakeGenericType(types);
                case 7:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,>).MakeGenericType(types);
                case 8:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,>).MakeGenericType(types);
                case 9:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,>).MakeGenericType(types);
                case 10:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,>).MakeGenericType(types);
                case 11:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,>).MakeGenericType(types);
                case 12:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,>).MakeGenericType(types);
                case 13:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,,>).MakeGenericType(types);
                case 14:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,,,>).MakeGenericType(types);
                case 15:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,,,,>).MakeGenericType(types);
                case 16:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,,,,,>).MakeGenericType(types);
                case 17:
                    if (resultType == typeof(void))
                        return typeof(Action<,,,,,,,,,,,,,,,>).MakeGenericType(typeParameters);
                    return typeof(Func<,,,,,,,,,,,,,,,,>).MakeGenericType(types);
                default:
                    throw new NotSupportedException();
            }
        }
    }
}

