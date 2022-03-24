using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TheWheel.Lambda;
using Microsoft.AspNetCore.Mvc.Formatters;

namespace TheWheel.OpenXml.WebApi
{
    public class XlsxMediaFormatter : TextOutputFormatter
    {
        private Func<string, string> translate;
        public XlsxMediaFormatter(Func<string, string> translate)
        {
            this.translate = translate;
        }

        public XlsxMediaFormatter()
            : this(s => s)
        {

        }

        protected override bool CanWriteType(Type type)
        {
            return type.IsEnumerable();
        }

        public override Task WriteResponseBodyAsync(OutputFormatterWriteContext context, Encoding selectedEncoding)
        {
            Extensions.Export(context.HttpContext.Response.Body, context.ObjectType.GetGenericArguments()[0], (IEnumerable)context.Object, null, translate);
            return Task.CompletedTask;
        }
    }
}
