using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class StreamTransport : ITransport<Stream>, IConfigurable<Stream, StreamTransport>
    {
        public StreamTransport() { }

        public StreamTransport(Stream options)
        {
            this.stream = options;
        }

        protected Stream stream;

        public StreamTransport Configure(Stream options)
        {
            this.stream = options;
            return this;
        }

        public virtual void Dispose()
        {
            if (this.stream != null)
                this.stream.Dispose();
        }

        public virtual Task<Stream> GetStreamAsync(CancellationToken token)
        {
            return Task.FromResult(this.stream);
        }

        public virtual Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            return Task.CompletedTask;
        }
    }


}