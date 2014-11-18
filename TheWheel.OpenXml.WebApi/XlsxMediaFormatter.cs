using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Formatting;
using System.Text;
using System.Threading.Tasks;
using TheWheel.Lambda;

namespace TheWheel.OpenXml.WebApi
{
    public class XlsxMediaFormatter : MediaTypeFormatter
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

        public override bool CanReadType(Type type)
        {
            return false;
        }

        public override bool CanWriteType(Type type)
        {
            return type.IsEnumerable();
        }

        public override Task WriteToStreamAsync(Type type, object value, System.IO.Stream writeStream, System.Net.Http.HttpContent content, System.Net.TransportContext transportContext)
        {

            Extensions.Export(writeStream, type.GetGenericArguments()[0], (IEnumerable)value, null, translate);
            return base.WriteToStreamAsync(type, value, writeStream, content, transportContext);
        }
    }
}
