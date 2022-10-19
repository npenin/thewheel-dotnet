using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Parlot;
using Parlot.Fluent;

namespace TheWheel.ETL.Parlot
{
    public class Context : ScopeParseContext<char, Context>
    {
        private IDictionary<string, ParameterExpression> variables = new Dictionary<string, ParameterExpression>();

        public Context(Context parent, Scanner<char> newScanner = null)
        : base(parent, newScanner)
        {

        }

        public Context(Scanner<char> scanner, bool useNewLines = false) : base(scanner, useNewLines)
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

        public override Context Scope(BufferSpan<char> buffer = default)
        {
            if (buffer.Buffer == null)
                return new Context(this);
            return new Context(this, new Scanner<char>(buffer));
        }
    }
}