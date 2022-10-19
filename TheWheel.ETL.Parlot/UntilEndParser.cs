
using System;
using Parlot;
using Parlot.Fluent;
using TContext = TheWheel.ETL.Parlot.Context;

namespace TheWheel.ETL.Parlot
{
    public sealed class UntilEndParser : Parser<BufferSpan<char>, TContext, char>
    {
        private readonly Parser<char, TContext> _end;

        public UntilEndParser(Parser<char, TContext> end)
        {
            _end = end;
        }

        public override bool Serializable => true;

        public override bool SerializableWithoutValue => false;

        public static UntilEndParser Build(Parser<char, TContext, char> end)
        {
            return new UntilEndParser(end);
        }

        public override bool Parse(TContext context, ref ParseResult<BufferSpan<char>> result)
        {
            context.EnterParser(this);

            var start = context.Scanner.Cursor.Offset;

            var parsedB = new ParseResult<char>();

            while (!_end.Parse(context, ref parsedB))
            {
                context.Scanner.Cursor.Advance();
            }

            result.Set(start, context.Scanner.Cursor.Offset, context.Scanner.Buffer.SubBuffer(start, context.Scanner.Cursor.Offset - start - 1));
            return true;
        }

        public override bool Serialize(BufferSpanBuilder<char> sb, BufferSpan<char> value)
        {
            sb.Append(value);
            return true;
        }
    }
}