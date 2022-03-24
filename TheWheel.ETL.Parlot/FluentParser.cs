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
using Parlot;
using System.Reflection;
using System.IO;

namespace TheWheel.ETL.Parlot
{

    public class FluentParser
    {
        public static async Task<IDataReceiver<TReceiveOptions>> To<TDataReceiver, TReceiveOptions>(Task<IDataProvider> provider, Task<TDataReceiver> destination, TReceiveOptions options)
        where TDataReceiver : IDataReceiver<TReceiveOptions>
        {
            return await destination.Receive(options, await provider);
        }

        public static async Task<IDataProvider> Query<T, TQuery>(Task<T> providerTask, TQuery query)
        where T : IAsyncQueryable<TQuery>
        {
            return await providerTask.Query(query); ;
        }

        public static bool Similar(Type reference, Type type)
        {
            if (reference.IsGenericParameter)
            {
                if (type.IsGenericParameter)
                    return reference.GenericParameterPosition == type.GenericParameterPosition;
                return true;
            }

            return ComparableType(reference) == ComparableType(type);

            Type ComparableType(Type cType)
                => cType.IsGenericType ? cType.GetGenericTypeDefinition() : cType;
        }
        public static MethodInfo GetMethodWithGenerics(
                                    Type type,
                                    string name, Type[] parameters,
                                    BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
        {
            var methods = type.GetMethods(flags);

            foreach (var method in methods)
            {
                var parmeterTypes = method.GetParameters().Select(p => p.ParameterType).ToArray();

                if (method.Name == name && parmeterTypes.Length == parameters.Length)
                {
                    bool match = true;

                    for (int i = 0; i < parameters.Length; i++)
                        match &= Similar(parmeterTypes[i], parameters[i]);

                    if (match)
                        return method;
                }
            }

            return null;
        }
        public static string GetName(string name)
        {
            return name;
        }
        public static readonly Parser<Expressions, Context> Expression;

        public static readonly TypeResolver TypeResolver = new TypeResolver();
        private static readonly Parser<Expressions, Context> DeclareStatement;

        private static readonly Parser<Expressions, Context> ImportAssembly;

        public static Parser<LambdaExpression, Context> SelectStatement { get; }

        static FluentParser()
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

            var divided = Terms.Char('/');
            var times = Terms.Char('*');
            var minus = Terms.Char('-');
            var plus = Terms.Char('+');
            var assign = Terms.Char('=');
            var dot = Terms.Char('.');
            var comma = Terms.Char(',');
            var lt = Terms.Char('<');
            var gt = Terms.Char('>');
            var semicolon = Terms.Char(';');
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


            ImportAssembly = reference.SkipAnd(assembly).SkipAnd(Terms.String(StringLiteralQuotes.Single)).And(ZeroOrOne(Terms.String(StringLiteralQuotes.Single))).Then<Expression>((c, s) =>
            {
                var resolve = new ResolveEventHandler((context, name) =>
                  {
                      var assemblyName = new AssemblyName(name.Name);
                      return Assembly.Load(File.ReadAllBytes(Path.Join(Path.GetDirectoryName(s.Item1.ToString()), assemblyName.Name + ".dll")));
                  });
                AppDomain.CurrentDomain.AssemblyResolve += resolve;

                var a = Assembly.Load(File.ReadAllBytes(s.Item1.ToString()));

                Load(a.GetReferencedAssemblies(), Path.GetDirectoryName(s.Item1.ToString()));

                AppDomain.CurrentDomain.AssemblyResolve -= resolve;
                if (s.Item2.Buffer != null)
                    TypeResolver.Using(a, s.Item2.ToString());

                return Expressions.Empty();
            });

            var dottedChain = Literals.Identifier().And(ZeroOrMany(dot.SkipAnd(Literals.Identifier()).Then(ts => ts.Length)).Then(ts => ts.Count == 0 ? -1 : ts[ts.Count - 1]));
            var classFullName = SkipWhiteSpace(new TypeParser(dot, Literals.Identifier(), TypeResolver));
            var type = Deferred<Type>();
            var generic = lt.SkipAnd(Separated(comma, type)).AndSkip(gt);
            type.Parser = classFullName.And(ZeroOrOne(generic)).Then(t => t.Item2 != null ? t.Item1.MakeGenericType(t.Item2.ToArray()) : t.Item1);
            var primitiveExpression = Deferred<Expression>();
            var expressionList = Separated(comma, primitiveExpression);
            var noParams = Expressions.Constant(null, typeof(KeyValuePair<string, object>[]));
            var invoke = openParen.SkipAnd(ZeroOrOne(expressionList)).AndSkip(closeParen);

            var variableReference = at.SkipAnd(Literals.Identifier()).Then<Expression>((c, t) => c.GetVariable(t.ToString()));
            var staticMemberReference = type.AndSkip(dot).And(Literals.Identifier()).And(ZeroOrOne(ZeroOrOne(generic).And(invoke))).Then<Expression>((c, t) =>
            {
                if (t.Item3.Item1 != null && t.Item3.Item2 == null)
                    throw new NotSupportedException();
                if (t.Item3.Item2 == null && t.Item3.Item1 == null)
                    return Expressions.MakeMemberAccess(null, t.Item1.GetMember(t.Item2.ToString())[0]);

                System.Reflection.MethodInfo mi;
                if (t.Item3.Item1 == null)
                    mi = t.Item1.GetMethod(t.Item2.ToString(), t.Item3.Item2.Select(static e => e.Type).ToArray());
                else
                    mi = GetMethodWithGenerics(t.Item1, t.Item2.ToString(), t.Item3.Item1.ToArray()).MakeGenericMethod(t.Item3.Item1.ToArray());

                return Expressions.Call(null, mi, t.Item3.Item2);
            });


            var newInstanciation = @new.SkipAnd(new NewAndAssign(type, invoke, primitiveExpression));
            var newArrayBySize = @new.SkipAnd(type).AndSkip(openBracket).And(integer).AndSkip(closeBracket).Then<Expression>(t => Expressions.NewArrayBounds(t.Item1, t.Item2));
            var newArrayByData = @new.SkipAnd(type).AndSkip(openBracket).AndSkip(closeBracket).AndSkip(openBrace).And(expressionList).AndSkip(closeBrace).Then<Expression>(t => Expressions.NewArrayInit(t.Item1, t.Item2));

            var providerInit = classFullName
                .And(ZeroOrOne(generic))
                .And(ZeroOrOne(invoke))
                .And(ZeroOrOne(invoke))
                .Then<Expression>(t =>
                {
                    System.Reflection.MethodInfo mi;
                    if (t.Item2 != null)
                        mi = GetMethodWithGenerics(t.Item1, "From", t.Item3.Select(static e => e.Type).ToArray()).MakeGenericMethod(t.Item2.ToArray());
                    else if (t.Item4 == null)
                        mi = t.Item1.GetMethod("From");
                    else
                        mi = t.Item1.GetMethod("From", t.Item3.Select(e => e.Type).ToArray());


                    MethodCallExpression from;
                    if (t.Item4 == null || t.Item4.Count == 0)
                    {
                        from = Expressions.Call(mi, t.Item3[0], noParams);
                        return Expressions.Call(typeof(FluentParser).GetMethod(nameof(FluentParser.Query)).MakeGenericMethod(from.Type.GetGenericArguments()[0] /*Task<XXX>*/, t.Item3[1].Type), from, t.Item3[1]);
                    }
                    else
                    {
                        from = Expressions.Call(mi, t.Item3);
                        return Expressions.Call(typeof(FluentParser).GetMethod(nameof(FluentParser.Query)).MakeGenericMethod(from.Type.GetGenericArguments()[0] /*Task<XXX>*/, t.Item4[0].Type), from, t.Item4[0]);
                    }
                })
                // .Then(exp => Expressions.Lambda<Func<Task<IDataProvider>>>(exp).Compile()())
                ;
            var receiverInit = classFullName.Then(t => t.GetMethod("To"))
                .And(ZeroOrOne(generic)).Then(t => t.Item2 != null ? t.Item1.MakeGenericMethod(t.Item2.ToArray()) : t.Item1)
                .And(invoke)
                .Then<(Expression, List<Expression>)>(t =>
                    {
                        if (t.Item1.GetParameters().Length == 1)
                            return new(Expressions.Call(t.Item1, t.Item2[0]), t.Item2);
                        return new(Expressions.Call(t.Item1, t.Item2[0], noParams), t.Item2);
                    }
                );

            var escapeSequence = openBracket.SkipAnd(new UntilEndParser<char>(closeBracket));
            var escapeOrNonEscapedSequence = escapeSequence.Or(Terms.Identifier());

            DeclareStatement = declare.And(at).And(Literals.Identifier()).And(classFullName).And(ZeroOrOne(assign.SkipAnd(primitiveExpression)))
                    .Then<Expression>((c, t) =>
                    {
                        var declaration = c.AddVariable(Expressions.Variable(t.Item4, t.Item3.ToString()));
                        if (t.Item5 == null)
                            return declaration;
                        return Expressions.Assign(declaration, Expressions.Convert(t.Item5, t.Item4));
                    });

            var aliasableExpressionList = Separated(comma, escapeOrNonEscapedSequence.And(ZeroOrOne(@as.SkipAnd(escapeOrNonEscapedSequence))).Then<Expression<Func<IDataRecord, IDataRecord>>>(t =>
             {
                 var newName = t.Item2.ToString();
                 var oldName = t.Item1.ToString();
                 var param = Expressions.Parameter(typeof(IDataRecord));
                 //  return record => new DataRecordRenamer(new DataRecordPicker(record, oldName), new Dictionary<string, string> { { oldName, newName } });
                 return Expressions.Lambda<Func<IDataRecord, IDataRecord>>(
                     Expressions.New(typeof(DataRecordRenamer).GetConstructor(new[] { typeof(IDataRecord), typeof(IDictionary<string, string>) }),
                        Expressions.New(typeof(DataRecordPicker).GetConstructor(new[] { typeof(IDataRecord), typeof(string[]) }), param, Expressions.NewArrayInit(typeof(string), Expressions.Constant(oldName))),
                     Expressions.ListInit(Expressions.New(typeof(Dictionary<string, string>)), Expressions.ElementInit(typeof(Dictionary<string, string>).GetMethod(nameof(Dictionary<string, string>.Add), 0, new[] { typeof(string), typeof(string) }), Expressions.Constant(oldName), Expressions.Constant(newName)))),
                     param);
             })
            .Or(Scope(c => { c.AddVariable(Expressions.Parameter(typeof(IDataRecord))); }, primitiveExpression.And(ZeroOrOne(@as.SkipAnd(escapeOrNonEscapedSequence)))
               .Then<Expression<Func<IDataRecord, IDataRecord>>>((c, t) =>
                {
                    var parameter = c.GetVariable("") ?? Expressions.Parameter(typeof(IDataRecord));
                    if (t.Item1 == starExpression)
                        if (t.Item2.Offset != 0 && t.Item2.Length > 0)
                            throw new EvaluateException("You cannot have * as " + t.Item2.ToString() + ". This is not a valid SQL expression");
                        else
                            return Expressions.Lambda<Func<IDataRecord, IDataRecord>>(parameter, parameter);

                    return Expressions.Lambda<Func<IDataRecord, IDataRecord>>(Expressions.Convert(Expressions.Call(null, new Func<string, object, DataRecord>(DataRecord.FromSingle).Method, Expressions.Constant(t.Item2.ToString(), typeof(string)), Expressions.Convert(t.Item1, typeof(object))), typeof(IDataRecord)), parameter);
                })
           )));

            var selectClause = select.SkipAnd(aliasableExpressionList).Then<Expression<Func<IDataProvider, Task<IDataProvider>>>>(t =>
             {
                 if (t.Count == 1 && t[0] == null)
                     return null;
                 Expression<Func<IDataRecord, IDataRecord>> func;
                 if (t.Count == 1)
                     func = t[0];
                 else
                 {
                     var funcs = t.Select(x => x == null ? record => record : x);
                     var param = Expressions.Parameter(typeof(IDataRecord));
                     var statements = funcs.Select(f => Expressions.Assign(f.Parameters[0], param)).ToList<Expression>();
                     statements.Add(Expressions.New(typeof(MultiDataRecord).GetConstructor(new Type[] { typeof(IDataRecord[]) }),
                        Expressions.NewArrayInit(typeof(IDataRecord), funcs.Select(f => f.Body))
                     ));
                     func = Expressions.Lambda<Func<IDataRecord, IDataRecord>>(Expressions.Block(funcs.SelectMany(f => f.Parameters), statements), param);
                 }

                 var provider = Expressions.Parameter(typeof(IDataProvider));
                 var transform = Expressions.Variable(typeof(TransformProvider));
                 return Expressions.Lambda<Func<IDataProvider, Task<IDataProvider>>>(Expressions.Block(
                     new[] { transform },
                     Expressions.Assign(transform, Expressions.New(typeof(TransformProvider))),
                     Expressions.Call(Expressions.Call(transform, nameof(TransformProvider.ReceiveAsync), null, provider, func),
                     nameof(Task.ContinueWith), new Type[] { typeof(IDataProvider) },
                        Expressions.Lambda<Func<Task, IDataProvider>>(Expressions.Convert(transform, typeof(IDataProvider)), Expressions.Parameter(typeof(Task))))
                 ), provider);
             });
            var intoClause = into.SkipAnd(receiverInit);
            var selectFromClause = from.SkipAnd(providerInit);

            SelectStatement = selectClause.And(intoClause).And(selectFromClause).Then<LambdaExpression>(t =>
                    {

                        var transform = t.Item1;
                        var from = t.Item3;
                        if (transform != null)
                            from = ContinueWith(from, transform);

                        var to = typeof(FluentParser).GetMethod(nameof(FluentParser.To)).MakeGenericMethod(t.Item2.Item1.Type.GetGenericArguments()[0] /*Task<XXXX>*/, t.Item2.Item2[1].Type);
                        return Expressions.Lambda(Expressions.Call(to,
                            from,
                            Expressions.Convert(t.Item2.Item1, to.GetParameters()[1].ParameterType),
                            Expressions.Convert(t.Item2.Item2[1], to.GetParameters()[2].ParameterType)
                        ));
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
            .Or(variableReference)
            .Or(newArrayBySize)
            .Or(newArrayByData)
            .Or(newInstanciation)
            .Or(staticMemberReference)
            .Or(escapeOrNonEscapedSequence.Then<Expression>((c, t) => Expressions.Call(c.GetVariable(""), GetValueMethodName(c), Type.EmptyTypes, Expressions.Call(c.GetVariable(""), nameof(IDataRecord.GetOrdinal), Type.EmptyTypes, Expressions.Constant(t.ToString())))))
            ;

            // The Recursive helper allows to create parsers that depend on themselves.
            // ( "-" ) unary | primary;
            var unary = Recursive<Expression>((u) =>
                minus.And(u)
                    .Then<Expression>(static x => Expressions.Negate(x.Item2))
                    .Or(primary));

            var memberAccess = unary.And(ZeroOrMany(dot.SkipAnd(Literals.Identifier()).And(ZeroOrOne(generic)).And(ZeroOrOne(invoke)))).Then<Expression>(static t =>
              {
                  var result = t.Item1;

                  if (t.Item2 != null)
                      foreach (var op in t.Item2)
                      {
                          if (op.Item3 != null)
                              if (op.Item2 != null)
                                  result = Expressions.Call(result, op.Item1.ToString(), op.Item2.ToArray(), op.Item3.ToArray());
                              else
                                  result = Expressions.Call(result, op.Item1.ToString(), null, op.Item3.ToArray());
                          else
                              result = Expressions.MakeMemberAccess(null, result.Type.GetMember(op.Item1.ToString())[0]);
                      }

                  return result;
              });


            // factor => unary ( ( "/" | "*" ) unary )* ;
            var factor = memberAccess.Then(static (c, t) => { if (t != null) c.PreviousType = t.Type; }).And(ZeroOrMany(divided.Or(times).And(memberAccess)))
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
            primitiveExpression.Parser = factor.Then((c, t) => { if (t != null) c.PreviousType = t.Type; }).And(ZeroOrMany(plus.Or(minus).And(factor)))
                        .Then(static (c, x) =>
                        {
                            // factor
                            var result = x.Item1;
                            // (("-" | "+") factor ) *
                            foreach (var op in x.Item2)
                            {
                                if (result.Type == typeof(string))
                                {
                                    result = op.Item1 switch
                                    {
                                        '+' => Expressions.Call(null, new Func<string, string, string>(string.Concat).Method, result, op.Item2),
                                        '-' => Expressions.Subtract(result, op.Item2),
                                        _ => null
                                    };
                                }
                                else
                                    result = op.Item1 switch
                                    {
                                        '+' => Expressions.Add(result, op.Item2),
                                        '-' => Expressions.Subtract(result, op.Item2),
                                        _ => null
                                    };
                            }

                            return result;
                        });





            Expression = ZeroOrMany(ImportAssembly.Or(DeclareStatement).Or(SelectStatement.Then(l => l.Body)).AndSkip(ZeroOrOne(semicolon))).Then<Expression>((c, t) => Expressions.Block(typeof(Task), c.GetScopeVariables(), t));
        }

        static byte[] msPublicKeyToken = new byte[] { 204, 123, 19, 255, 205, 45, 221, 81 };

        private static void Load(AssemblyName[] assemblyNames, string path, List<AssemblyName> loadedAssemblies = null)
        {
            if (loadedAssemblies == null)
                loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies().Select(a => a.GetName()).ToList();
            foreach (var name in assemblyNames)
            {
                if (loadedAssemblies.Any(loadedAssembly => loadedAssembly.FullName == name.FullName))
                    continue;
                if (name.Name == "netstandard" && name.GetPublicKeyToken().SequenceEqual(msPublicKeyToken))
                    continue;

                var a = Assembly.Load(name);
                loadedAssemblies.Add(name); //preventing circular references
                Load(a.GetReferencedAssemblies(), path, loadedAssemblies);
            }
        }

        private static Expressions ContinueWith(Expressions from, Expression<Func<IDataProvider, Task<IDataProvider>>> methodCallExpression)
        {
            var provider = Expressions.Parameter(typeof(Task<IDataProvider>));
            return Expressions.Call(typeof(TaskExtensions), nameof(TaskExtensions.Unwrap), new[] { typeof(IDataProvider) },
            Expressions.Call(from, nameof(Task<IDataProvider>.ContinueWith), new[] { typeof(Task<IDataProvider>) },
            Expressions.Lambda<Func<Task<IDataProvider>, Task<IDataProvider>>>(
                Expressions.Block(methodCallExpression.Parameters,
                    Expressions.Assign(methodCallExpression.Parameters[0], Expressions.Property(provider, nameof(Task<IDataProvider>.Result))),
                    methodCallExpression.Body), provider)));
        }

        private static string GetValueMethodName(Context c)
        {
            if (c.PreviousType != null && MethodMapping.TryGetValue(c.PreviousType, out var result))
                return result;
            return MethodMapping[typeof(object)];
        }

        private static IDictionary<Type, string> MethodMapping = new Dictionary<Type, string>
        {
            {typeof(short), nameof(IDataRecord.GetInt16)},
            {typeof(int), nameof(IDataRecord.GetInt32)},
            {typeof(long), nameof(IDataRecord.GetInt64)},
            {typeof(DateTime), nameof(IDataRecord.GetDateTime)},
            {typeof(decimal), nameof(IDataRecord.GetDecimal)},
            {typeof(double), nameof(IDataRecord.GetDouble)},
            {typeof(float), nameof(IDataRecord.GetFloat)},
            {typeof(Guid), nameof(IDataRecord.GetGuid)},
            {typeof(string), nameof(IDataRecord.GetString)},
            {typeof(object), nameof(IDataRecord.GetValue)},
        };
    }
}