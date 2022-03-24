using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Parlot;
using Parlot.Fluent;

namespace TheWheel.ETL.Parlot
{
    public class Context : ScopeParseContext<Context>
    {
        private IDictionary<string, ParameterExpression> variables = new Dictionary<string, ParameterExpression>();

        public Context(Context parent)
        : base(parent)
        {

        }

        public Context(Scanner scanner, bool useNewLines = false) : base(scanner, useNewLines)
        {
        }

        public Type PreviousType { get; set; }

        public ParameterExpression AddVariable(ParameterExpression variable)
        {
            this.variables.Add(variable.Name ?? "", variable);
            return variable;
        }

        public ParameterExpression GetVariable(string name)
        {
            if (variables.TryGetValue(name, out var result))
                return result;
            if (parent != null)
                return parent.GetVariable(name);
            return null;
        }

        public IEnumerable<ParameterExpression> GetScopeVariables()
        {
            return variables.Values;
        }

        public override Context Scope()
        {
            return new Context(this);
        }
    }
}