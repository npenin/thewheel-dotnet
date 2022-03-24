using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Lambda
{
    class PathifyExpressionVisitor : ExpressionVisitor
    {
        private PathifyExpressionVisitor()
        {

        }

        StringBuilder sb = new StringBuilder();

        public static string Process(Expression exp)
        {
            var visitor = new PathifyExpressionVisitor();
            visitor.Visit(exp);
            return visitor.sb.ToString();
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null && node.Expression.NodeType != ExpressionType.Parameter && (node.Expression.NodeType != ExpressionType.Convert || ((UnaryExpression)node.Expression).Operand.NodeType != ExpressionType.Parameter))
            {
                Visit(node.Expression);
                sb.Append(".");
            }
            sb.Append(node.Member.Name);
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "SelectMany" || node.Method.Name == "Select")
            {
                Visit(node.Arguments.First());
                sb.Append(".");

                Visit(node.Arguments.Skip(1).First());

                return node;
            }
            else
                throw new NotSupportedException("No method other than SelectMany is supported");
        }
    }
}
