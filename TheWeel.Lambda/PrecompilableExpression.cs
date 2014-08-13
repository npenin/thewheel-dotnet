using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace TheWheel.Lambda
{
    public class PrecompilableExpression<T>
    {
        private Expression<T> lambda;
        private Expression<T> processedLambda;
        private bool evaluating;
        public PrecompilableExpression(Expression<T> lambda)
        {
            this.lambda = lambda;
        }

        public static implicit operator PrecompilableExpression<T>(Expression<T> lambda)
        {
            return new PrecompilableExpression<T>(lambda);
        }

        public static implicit operator Expression<T>(PrecompilableExpression<T> compilable)
        {
            return compilable.AsExpression();
        }

        public static explicit operator Expression(PrecompilableExpression<T> compilable)
        {
            return compilable.AsExpression();
        }

        public Expression<T> AsExpression()
        {
            if (processedLambda == null)
            {
                if (evaluating)
                    return null;// AsExpression(lambda);
                evaluating = true;
                bool requiresReprocessing = true;
                while (requiresReprocessing)
                    processedLambda = CompilableVisitor.Process(lambda, out requiresReprocessing);
                evaluating = false;
            }
            return processedLambda;
        }

        private static Expression<Func<U, V>> AsExpression<U, V>(Expression<Func<U, V>> lambda)
        {
            return lambda.Update(new Func<Expression<Func<U, V>>, Expression<Func<U, V>>>(AsExpression).Method.Call(lambda), lambda.Parameters);
        }
    }
}
