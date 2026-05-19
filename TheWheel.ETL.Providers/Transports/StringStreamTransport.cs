using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class StringStreamTransport : StreamTransport, ITransport<Stream>, IConfigurable<string, StringStreamTransport>
    {
        public StringStreamTransport() { }

        public StringStreamTransport(string source)
        {
            this.stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(source));
        }

        public StringStreamTransport Configure(string source)
        {
            this.stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(source));
            return this;
        }

        public override Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            if (this.stream == null && !string.IsNullOrEmpty(connectionString))
                this.stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(connectionString));
            return Task.CompletedTask;
        }
    }


}