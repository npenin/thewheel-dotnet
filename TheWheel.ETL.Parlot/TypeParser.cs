using System;
using Parlot;
using Parlot.Fluent;

namespace TheWheel.ETL.Parlot
{
    public class TypeParser : Parser<Type, Context, char>
    {
        private Parser<char, Context> separator;
        private Parser<BufferSpan<char>, Context> identifier;
        private TypeResolver resolver;

        public TypeParser(Parser<char, Context> separator, Parser<BufferSpan<char>, Context> identifier, TypeResolver resolver)
        {
            this.separator = separator;
            this.identifier = identifier;
            this.resolver = resolver;
        }

        public override bool Serializable => true;

        public override bool SerializableWithoutValue => false;

        public override bool Parse(Context context, ref ParseResult<Type> result)
        {
            context.EnterParser(this);

            var span = new ParseResult<BufferSpan<char>>();
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


                type = resolver.Get(context.Scanner.Buffer.AsSpan(start, span.End - start));
            }

            if (type != null)
            {
                result.Set(start, span.End, type);
                return true;
            }

            return false;
        }

        public override bool Serialize(BufferSpanBuilder<char> sb, Type value)
        {
            sb.Append(value.FullName.AsSpan());
            return true;
        }
    }
}