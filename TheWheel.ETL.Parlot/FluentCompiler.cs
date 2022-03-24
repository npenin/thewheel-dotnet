using Parlot.Fluent;
using static Parlot.Fluent.Parsers<TheWheel.ETL.Parlot.Context>;
using System.Linq.Expressions;
using Expressions = System.Linq.Expressions.Expression;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Fluent;
using System.Data;

namespace TheWheel.ETL.Parlot
{

    public class FluentCompiler
    {
        public static async Task<IDataReceiver<TReceiveOptions>> To<TDataProvider, TReceiveOptions>(Task<TDataProvider> provider, Task<IDataReceiver<TReceiveOptions>> destination, TReceiveOptions options)
        where TDataProvider : IDataProvider
        {
            return await destination.Receive(options, await provider);
        }

        public static async Task<IDataProvider> Query<T, TQuery>(Task<T> providerTask, TQuery query)
        where T : IAsyncQueryable<TQuery>
        {
            return await providerTask.Query(query); ;
        }

        public static string GetName(string name)
        {
            return name;
        }
        public static readonly Parser<Expression, Context> Expression;

        public static readonly TypeResolver TypeResolver = new TypeResolver();

        static FluentCompiler()
        {
            /*
             * Grammar:
             * expression     => factor ( ( "-" | "+" ) factor )* ;
             * factor         => unary ( ( "/" | "*" ) unary )* ;
             * unary          => ( "-" ) unary
             *                 | primary ;
             * primary        => NUMBER
             *                  | "(" expression ")" ;
            */

            // The Deferred helper creates a parser that can be referenced by others before it is defined
            var deferred = Deferred<Expression>();

            var number = Terms.Decimal()
                .Then<Expression>(static d => Expressions.Constant(d))
                ;
            var integer = Terms.Decimal()
                .Then<Expression>(static d => Expressions.Constant((int)d))
                ;

            var starExpression = Expressions.Constant('*');

            var star = Terms.Char('*').Then<Expression>(t => starExpression);

            var identifier = Terms.Identifier();


            var divided = Terms.Char('/');
            var times = Terms.Char('*');
            var minus = Terms.Char('-');
            var plus = Terms.Char('+');
            var assign = Terms.Char('=');
            var dot = Terms.Char('.');
            var comma = Terms.Char(',');
            var lt = Terms.Char('<');
            var gt = Terms.Char('>');
            var openParen = Terms.Char('(');
            var closeParen = Terms.Char(')');
            var openBracket = Terms.Char('[');
            var closeBracket = Terms.Char(']');
            var openBrace = Terms.Char('{');
            var closeBrace = Terms.Char('}');
            var at = Terms.Char('@');
            var @new = Terms.Text("new", false);
            var @true = Terms.Text("true", false).Then<Expression>(static t => Expressions.Constant(true));
            var @false = Terms.Text("false", false).Then<Expression>(static t => Expressions.Constant(false));
            var @null = Terms.Text("null", false).Then<Expression>(static t => Expressions.Constant(null, typeof(object)));
            var @NULL = Terms.Text("NULL", false).Then<Expression>(static t => Expressions.Constant(DBNull.Value));
            var @as = Terms.Text("AS", true);
            var from = Terms.Text("FROM", true);
            var select = Terms.Text("SELECT", true);
            var into = Terms.Text("INTO", true);
            var join = Terms.Text("JOIN", true);
            var inner = Terms.Text("INNER", true);
            var @if = Terms.Text("IF", true);
            var @else = Terms.Text("ELSE", true);
            var begin = Terms.Text("BEGIN", true);
            var end = Terms.Text("END", true);
            var use = Terms.Text("USE", true);
            var declare = Terms.Text("DECLARE", true);
            var reference = Terms.Text("REFERENCE", true);
            var assembly = Terms.Text("ASSEMBLY", true);

            var dottedChain = Terms.Identifier().And(ZeroOrMany(dot.SkipAnd(Terms.Identifier()).Then(ts => ts.Length)).Then(ts => ts.Count == 0 ? -1 : ts[ts.Count - 1]));
            var classFullName = new TypeParser(dot, Terms.Identifier(), TypeResolver);
            var generic = lt.SkipAnd(Separated(comma, classFullName)).AndSkip(gt);
            var type = classFullName.And(generic).Then(t => t.Item1.MakeGenericType(t.Item2.ToArray())).Or(classFullName);
            var primitiveExpression = Deferred<Expression>();
            var expressionList = Separated(comma, primitiveExpression);
            var noParams = Expressions.Constant(null, typeof(KeyValuePair<string, object>[]));

            var staticMemberReference = type.AndSkip(dot).And(Terms.Identifier()).Then<Expression>(t => Expressions.MakeMemberAccess(null, t.Item1.GetMember(t.Item2.ToString())[0]));

            var invoke = openParen.SkipAnd(expressionList).AndSkip(closeParen);

            var newInstanciation = @new.SkipAnd(new NewAndAssign(type, invoke, primitiveExpression));
            var newArrayBySize = @new.SkipAnd(type).AndSkip(openBracket).And(integer).AndSkip(closeBracket).Then<Expression>(t => Expressions.NewArrayBounds(t.Item1, t.Item2));
            var newArrayByData = @new.SkipAnd(type).AndSkip(openBracket).AndSkip(closeBracket).AndSkip(openBrace).And(expressionList).AndSkip(closeBrace).Then<Expression>(t => Expressions.NewArrayInit(t.Item1, t.Item2));

            var providerInit = classFullName.Then(t => t.GetMethod("From"))
                .And(ZeroOrOne(generic)).Then(t => t.Item2 != null ? t.Item1.MakeGenericMethod(t.Item2.ToArray()) : t.Item1)
                .And(invoke)
                .Then(t =>
                {
                    var from = Expressions.Call(t.Item1, t.Item2[0], noParams);
                    return Expressions.Call(typeof(FluentParser).GetMethod("Query").MakeGenericMethod(from.Type.GetGenericArguments()[0] /*Task<XXX>*/, t.Item2[1].Type), from, t.Item2[1]);
                });
            var receiverInit = classFullName.Then(t => t.GetMethod("To"))
                .And(ZeroOrOne(generic)).Then(t => t.Item2 != null ? t.Item1.MakeGenericMethod(t.Item2.ToArray()) : t.Item1)
                .And(invoke)
                .Then<(Expression, List<Expression>)>(t => new(Expressions.Call(t.Item1, t.Item2[0], noParams), t.Item2));

            var escapeSequence = openBracket.SkipAnd(new UntilEndParser<char>(closeBracket));
            var escapeOrNonEscapedSequence = escapeSequence.Or(Terms.Identifier());
            var referenceAssemblyStatement = reference.SkipAnd(assembly).SkipAnd(escapeOrNonEscapedSequence).Then(t =>
            {
                var assemblyName = t.ToString();
                return (Expression<Action>)(() => System.Reflection.Assembly.LoadFrom(assemblyName));
            });

            var declareStatement = declare.And(at).And(Terms.Identifier()).And(classFullName).Then((c, t) =>
                c.AddVariable(Expressions.Variable(t.Item4, t.Item3.ToString()))
            );

            var aliasableExpressionList = Separated(comma, Terms.Identifier().Then<Expression>(t => Expressions.Call(typeof(FluentParser), "GetName", null, Expressions.Constant(t.ToString()))).Or(primitiveExpression).Or(primitiveExpression.AndSkip(@as).And(escapeOrNonEscapedSequence)
                .Then<Expression>(t => Expressions.Assign(Expressions.Constant(t.Item2), t.Item1))
            ));

            var selectClause = select.SkipAnd(aliasableExpressionList).Then<Expression>(t =>
            {
                if (t.Count == 1 && t[0].NodeType == ExpressionType.Constant && t[0] == starExpression)
                    return starExpression;
                if (t.All(exp => exp.NodeType == ExpressionType.Call && ((MethodCallExpression)exp).Method.Name == "GetName" && ((MethodCallExpression)exp).Method.DeclaringType == typeof(FluentParser)))
                    return Expressions.New(typeof(DataRecordPicker).GetConstructor(new[] { typeof(IDataRecord), typeof(string[]) }), new Expression[] { Expressions.Constant(null, typeof(DataRecord)), Expressions.NewArrayInit(typeof(string), t.Select(exp => ((MethodCallExpression)exp).Arguments[0]).ToArray()) });
                throw new NotSupportedException();
            });
            var intoClause = into.SkipAnd(receiverInit);
            var selectFromClause = from.SkipAnd(providerInit);
            var selectStatement = selectClause.And(intoClause).And(selectFromClause).Then<Expression>(t =>
            {
                if (t.Item1 != null && t.Item1.NodeType == ExpressionType.Constant && t.Item1 == starExpression) //SELECT *
                {
                    var to = typeof(FluentParser).GetMethod("To").MakeGenericMethod(t.Item3.Type.GetGenericArguments()[0] /*Task<IDataProvider>*/, t.Item2.Item2[1].Type);
                    return Expressions.Call(to,
                        Expressions.Convert(t.Item3, to.GetParameters()[0].ParameterType),
                        Expressions.Convert(t.Item2.Item1, to.GetParameters()[1].ParameterType),
                        Expressions.Convert(t.Item2.Item2[1], to.GetParameters()[2].ParameterType)
                    );
                }
                else if (t.Item1 != null)
                {
                    var parameter = Expressions.Parameter(typeof(IDataRecord));
                    var from = Expressions.New(typeof(TransformReader).GetConstructor(new[] { typeof(IDataReader), typeof(Func<IDataRecord, IDataRecord>) }),
                        t.Item3,
                        Expressions.Lambda<Func<IDataRecord, IDataRecord>>(
                            Expressions.New(typeof(DataRecordPicker).GetConstructor(new[] { typeof(IDataRecord), typeof(string[]) }),
                                parameter,
                                ((NewExpression)t.Item1).Arguments[1]
                            )
                        , parameter)
                    );
                    var to = typeof(FluentParser).GetMethod("To").MakeGenericMethod(t.Item3.Type.GetGenericArguments()[0] /*Task<IDataProvider>*/, t.Item2.Item2[1].Type);
                    return Expressions.Call(to,
                        Expressions.Convert(t.Item3, to.GetParameters()[0].ParameterType),
                        Expressions.Convert(t.Item2.Item1, to.GetParameters()[1].ParameterType),
                        Expressions.Convert(t.Item2.Item2[1], to.GetParameters()[2].ParameterType)
                    );
                }
                else
                    throw new NotSupportedException();
            });

            // "(" expression ")"
            var groupExpression = Between(openParen, primitiveExpression, closeParen);

            // primary => NUMBER | "(" expression ")";
            var primary = number
            .Or(star)
            .Or(Terms.String(StringLiteralQuotes.Single).Then<Expression>(t => Expressions.Constant(t.ToString())))
            .Or(groupExpression)
            .Or(@true)
            .Or(@false)
            .Or(@null)
            .Or(staticMemberReference)
            .Or(newArrayBySize)
            .Or(newArrayByData)
            .Or(newInstanciation)
            ;

            // The Recursive helper allows to create parsers that depend on themselves.
            // ( "-" ) unary | primary;
            var unary = Recursive<Expression>((u) =>
                minus.And(u)
                    .Then<Expression>(static x => Expressions.Negate(x.Item2))
                    .Or(primary));

            // factor => unary ( ( "/" | "*" ) unary )* ;
            var factor = unary.And(ZeroOrMany(divided.Or(times).And(unary)))
                .Then(static x =>
                {
                    // unary
                    var result = x.Item1;

                    // (("/" | "*") unary ) *
                    foreach (var op in x.Item2)
                    {
                        result = op.Item1 switch
                        {
                            '/' => Expressions.Divide(result, op.Item2),
                            '*' => Expressions.Multiply(result, op.Item2),
                            _ => null
                        };
                    }

                    return result;
                });

            // expression => factor ( ( "-" | "+" ) factor )* ;
            primitiveExpression.Parser = factor.And(ZeroOrMany(plus.Or(minus).And(factor)))
                            .Then(static x =>
                            {
                                // factor
                                var result = x.Item1;

                                // (("-" | "+") factor ) *
                                foreach (var op in x.Item2)
                                {
                                    result = op.Item1 switch
                                    {
                                        '+' => Expressions.Add(result, op.Item2),
                                        '-' => Expressions.Subtract(result, op.Item2),
                                        _ => null
                                    };
                                }

                                return result;
                            });

            Expression = selectStatement;
        }
    }

}