using System;
using Parlot;
using Parlot.Fluent;

namespace TheWheel.ETL.Parlot
{
    public class TypeParser : Parser<Type, Context>
    {
        private Parser<char, Context> separator;
        private Parser<TextSpan, Context> identifier;
        private TypeResolver resolver;

        public TypeParser(Parser<char, Context> separator, Parser<TextSpan, Context> identifier, TypeResolver resolver)
        {
            this.separator = separator;
            this.identifier = identifier;
            this.resolver = resolver;
        }

        public override bool Parse(Context context, ref ParseResult<Type> result)
        {
            context.EnterParser(this);

            var span = new ParseResult<TextSpan>();
            var separatorResult = new ParseResult<char>();
            if (!identifier.Parse(context, ref span))
                return false;

            var start = span.Start;


            var type = resolver.Get(span.Value.ToString());

            while (type == null)
            {
                if (!separator.Parse(context, ref separatorResult))
                    return false;

                if (!identifier.Parse(context, ref span))
                    return false;


                type = resolver.Get(context.Scanner.Buffer.Substring(start, span.End - start));
            }

            if (type != null)
            {
                result.Set(start, span.End, type);
                return true;
            }

            return false;
        }
    }
}