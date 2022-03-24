using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parlot;
using Parlot.Fluent;
using TContext = TheWheel.ETL.Parlot.Context;

namespace TheWheel.ETL.Parlot
{
    public class NewAndAssign : Parser<Expression, TContext>
    {
        Parser<Type, TContext> type;
        Parser<char, TContext> memberInitOpen;
        Parser<char, TContext> memberInitClose;
        Parser<TextSpan, TContext> memberIdentifier;
        private Parser<char, TContext> assignParser;
        private Parser<char, TContext> memberSeparatorParser;
        private Parser<Expression, TContext> expressionParser;
        private Parser<List<Expression>, TContext> ctorArgs;

        public NewAndAssign(Parser<Type, TContext> type, Parser<List<Expression>, TContext> ctorArgs, Parser<Expression, TContext> expressionParser)
        {
            this.type = type;
            this.memberInitOpen = Parsers<TContext>.Terms.Char('{');
            this.memberInitClose = Parsers<TContext>.Terms.Char('}');
            this.memberIdentifier = Parsers<TContext>.Terms.Identifier();
            this.assignParser = Parsers<TContext>.Terms.Char('=');
            this.memberSeparatorParser = Parsers<TContext>.Terms.Char(',');
            this.expressionParser = expressionParser;
            this.ctorArgs = ctorArgs;
        }

        public override bool Parse(TContext context, ref ParseResult<Expression> result)
        {
            context.EnterParser(this);

            var type = new ParseResult<Type>();
            if (!this.type.Parse(context, ref type))
                return false;

            NewExpression newExp;
            var ctorArgs = new ParseResult<List<Expression>>();

            if (this.ctorArgs.Parse(context, ref ctorArgs) && ctorArgs.Value != null)
            {
                ConstructorInfo ctor = type.Value.GetConstructor(ctorArgs.Value.Select(exp => exp.Type).ToArray());
                ParameterInfo[] parameters;
                if (ctor == null)
                {
                    var ctors = type.Value.GetConstructors();
                    if (ctors.Length == 1)
                    {
                        ctor = ctors[0];
                        parameters = ctor.GetParameters();
                    }
                    else
                    {
                        float confidence = 0f;
                        foreach (var c in ctors)
                        {
                            parameters = c.GetParameters();
                            var cfidence = 0f;
                            for (int i = 0; i < parameters.Length; i++)
                            {
                                if (parameters[i].ParameterType == ctorArgs.Value[i].Type)
                                    cfidence += 1f / parameters.Length;
                                if (ctorArgs.Value[i].Type.IsAssignableTo(parameters[i].ParameterType))
                                    cfidence += .9f / parameters.Length;
                            }
                            if (cfidence > confidence)
                                ctor = c;
                        }
                        parameters = ctor.GetParameters();
                    }
                    newExp = Expression.New(ctor, ctorArgs.Value.Select((e, i) => Expression.Convert(e, parameters.ElementAt(i).ParameterType)));
                }
                else
                {
                    parameters = ctor.GetParameters();
                    newExp = Expression.New(ctor, ctorArgs.Value.Select((e, i) => e.Type != parameters[i].ParameterType ? Expression.Convert(e, parameters[i].ParameterType) : e));
                }
            }
            else
                newExp = Expression.New(type.Value);

            var charResult = new ParseResult<char>();
            if (!memberInitOpen.Parse(context, ref charResult))
            {
                result.Set(type.Start, type.End, newExp);
                return true;
            }

            var expressionParsed = new ParseResult<Expression>();
            var memberInit = new List<MemberAssignment>();

            do
            {
                var span = new ParseResult<TextSpan>();
                if (!memberIdentifier.Parse(context, ref span))
                    return false;

                var member = type.Value.GetMember(span.Value.ToString());
                if (member == null || member.Length == 0)
                    return false;

                if (!this.assignParser.Parse(context, ref charResult))
                    return false;

                if (!this.expressionParser.Parse(context, ref expressionParsed))
                    return false;

                Type memberType;
                if (member[0] is PropertyInfo pi)
                    memberType = pi.PropertyType;
                else if (member[0] is FieldInfo fi)
                    memberType = fi.FieldType;
                else
                    throw new NotSupportedException(member[0].GetType() + " is not a supported MemberInfo");


                if (expressionParsed.Value.Type == memberType)
                    memberInit.Add(Expression.Bind(member[0], expressionParsed.Value));
                else
                    memberInit.Add(Expression.Bind(member[0], Expression.Convert(expressionParsed.Value, memberType)));
            }
            while (memberSeparatorParser.Parse(context, ref charResult));

            if (memberInitClose.Parse(context, ref charResult))
            {
                result.Set(type.Start, charResult.End, Expression.MemberInit(newExp, memberInit.ToArray()));
                return true;
            }

            return false;
        }
    }
}