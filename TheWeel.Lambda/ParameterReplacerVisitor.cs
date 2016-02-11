using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.Lambda
{
    public class ParameterReplacerVisitor : ExpressionVisitor
    {
        private ParameterReplacerVisitor(Expression oldParam, ParameterExpression newParam, Expression newExp)
        {
            this.oldParam = oldParam;
            this.newParam = newParam;
            this.newExp = newExp;
        }

        private Expression oldParam;
        private Expression newParam;
        private Expression newExp;

        public static Expression Process(Expression exp, Expression oldParam, ParameterExpression newParam, Expression newExp)
        {
            return new ParameterReplacerVisitor(oldParam, newParam, newExp).Visit(exp);
        }

        public static Expression Process(Expression exp, Expression oldParam, ParameterExpression newParam)
        {
            return new ParameterReplacerVisitor(oldParam, newParam, newParam).Visit(exp);
        }

        public static Expression Process(LambdaExpression exp, ParameterExpression newParam)
        {
            return Process(exp, newParam, (Expression)newParam);
        }

        public static Expression<T> Process<T>(Expression<T> exp, ParameterExpression newParam)
        {
            return (Expression<T>)Process((LambdaExpression)exp, newParam);
        }

        public static Expression Process(LambdaExpression exp, ParameterExpression newParam, Expression newExp)
        {
            return Process((Expression)exp, exp.Parameters.First(), newParam, newExp);
        }



        //public static LambdaExpression Process(LambdaExpression exp, ParameterExpression newParam)
        //{
        //    return Process(exp, exp.Parameters.First(), newParam);
        //}

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var exp = node.Value as Expression;
            if (exp != null)
            {
                var newExp = Visit(exp);
                if (exp != newExp)
                    return Expression.Constant(newExp);
                return newExp;
            }
            return base.VisitConstant(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var body = Visit(node.Body);
            var parameters = this.MyVisitAndConvert<ParameterExpression>(node.Parameters);
            if (parameters == null)
                return body;
            if (parameters.Where((p, i) => typeof(T).GetGenericArguments()[i] == p.Type).Count() == parameters.Count)
                return Expression.Lambda<T>(body, parameters);
            Type delegateType = null;
            switch (parameters.Count)
            {
                case 0:
                    delegateType = typeof(Func<>);
                    break;
                case 1:
                    delegateType = typeof(Func<,>);
                    break;
                case 2:
                    delegateType = typeof(Func<,,>);
                    break;
                case 3:
                    delegateType = typeof(Func<,,,>);
                    break;
                case 4:
                    delegateType = typeof(Func<,,,,>);
                    break;
                case 5:
                    delegateType = typeof(Func<,,,,,>);
                    break;
                case 6:
                    delegateType = typeof(Func<,,,,,,>);
                    break;
                case 7:
                    delegateType = typeof(Func<,,,,,,,>);
                    break; ;
                case 8:
                    delegateType = typeof(Func<,,,,,,,,>);
                    break;
                case 9:
                    delegateType = typeof(Func<,,,,,,,,,>);
                    break;
                case 10:
                    delegateType = typeof(Func<,,,,,,,,,,>);
                    break;
                case 11:
                    delegateType = typeof(Func<,,,,,,,,,,,>);
                    break;
                case 12:
                    delegateType = typeof(Func<,,,,,,,,,,,,>);
                    break;
                case 13:
                    delegateType = typeof(Func<,,,,,,,,,,,,,>);
                    break;
                case 14:
                    delegateType = typeof(Func<,,,,,,,,,,,,,,>);
                    break;
                case 15:
                    delegateType = typeof(Func<,,,,,,,,,,,,,,,>);
                    break;
                case 16:
                    delegateType = typeof(Func<,,,,,,,,,,,,,,,,>);
                    break;
            }
            if (delegateType == null)
                return Expression.Lambda(body, parameters);
            return Expression.Lambda(delegateType.MakeGenericType(typeof(T).GetGenericArguments().Select((t, i) => i < parameters.Count ? parameters[i].Type : t).ToArray()), body, parameters);
        }

        private ReadOnlyCollection<T> MyVisitAndConvert<T>(ReadOnlyCollection<T> nodes)
            where T : Expression
        {
            T[] array = null;
            int i = 0;
            int count = nodes.Count;
            while (i < count)
            {
                T t = this.Visit(nodes[i]) as T;
                if (nodes[i] == oldParam)
                    t = newParam as T;
                if (t == null)
                {
                    count--;
                }
                if (array != null)
                {
                    array[i] = t;
                }
                else
                {
                    if (!object.ReferenceEquals(t, nodes[i]))
                    {
                        array = new T[count];
                        for (int j = 0; j < i; j++)
                        {
                            array[j] = nodes[j];
                        }
                        if (t != null)
                            array[i] = t;
                    }
                }
                i++;
            }
            if (array == null)
            {
                return nodes;
            }
            if (array.Length == 0)
                return null;
            return new ReadOnlyCollection<T>(array);
        }

        public override Expression Visit(Expression node)
        {
            if (node == oldParam)
                return newExp;
            return base.Visit(node);
        }
    }
}
