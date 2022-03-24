
using Parlot;
using Parlot.Fluent;
using TContext = TheWheel.ETL.Parlot.Context;

namespace TheWheel.ETL.Parlot
{
    public sealed class UntilEndParser<TEnd> : Parser<TextSpan, TContext>
    {
        private readonly Parser<TEnd, TContext> _end;

        public UntilEndParser(Parser<TEnd, TContext> end)
        {
            _end = end;
        }

        public static UntilEndParser<XEnd> Build<XEnd>(Parser<XEnd, TContext> end)
        {
            return new UntilEndParser<XEnd>(end);
        }

        public override bool Parse(TContext context, ref ParseResult<TextSpan> result)
        {
            context.EnterParser(this);

            var start = context.Scanner.Cursor.Offset;

            var parsedB = new ParseResult<TEnd>();

            while (!_end.Parse(context, ref parsedB))
            {
                context.Scanner.Cursor.Advance();
            }

            result.Set(start, context.Scanner.Cursor.Offset, new TextSpan(context.Scanner.Buffer, start, context.Scanner.Cursor.Offset - start - 1));
            return true;
        }
    }
}