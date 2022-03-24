using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.Lambda
{
    public class CompilableVisitor : ExpressionVisitor
    {
        private static MethodInfo[] asCompilable = typeof(ReflectionExpression).GetMethods().Where(mi => mi.Name == "AsCompilable").ToArray();
        private static MethodInfo[] asLambda = typeof(ReflectionExpression).GetMethods().Where(mi => mi.Name == "AsLambda").ToArray();
        private ParameterExpression parameter;
        private bool requiresReProcessing;

        private CompilableVisitor()
        {
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            LambdaExpression lambda;
            if (node.Method.IsGenericMethod && asCompilable.Contains(node.Method.GetGenericMethodDefinition()))
            {
                var result = Visit(node.Arguments[0]);
                if (result.NodeType == ExpressionType.Lambda)
                {
                    lambda = (LambdaExpression)result;
                    return lambda.Replace(parameter).Body;
                }
                requiresReProcessing = true;
                return node;
            }

            if (node.Method.IsGenericMethod && asLambda.Contains(node.Method.GetGenericMethodDefinition()))
            {
                lambda = (LambdaExpression)node.Arguments[1].PartialEval();
                if (lambda == null)
                {
                    requiresReProcessing = true;
                    return node;
                }
                return lambda.Replace(parameter, node.Arguments[0]);
            }
            if (node.Method.Name == "AsExpression" && node.Object.Type.IsGenericType && node.Object.Type.GetGenericTypeDefinition() == typeof(PrecompilableExpression<>))
            {
                return node.PartialEval();
            }
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var oldParam = parameter;
            parameter = node.Parameters[0];
            var result = base.VisitLambda<T>(node);
            parameter = oldParam;
            return result;
        }


        public static Expression<T> Process<T>(Expression<T> expression, out bool requiresReprocessing)
        {
            var visitor = new CompilableVisitor();
            var result = (Expression<T>)visitor.Visit(expression);
            requiresReprocessing = visitor.requiresReProcessing;
            return result;
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
        {
            return base.VisitMemberAssignment(node);
        }
    }
}
