using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class StreamTransport : ITransport<Stream>, IConfigurable<Stream, StreamTransport>
    {
        private Stream stream;

        public StreamTransport Configure(Stream options)
        {
            this.stream = options;
            return this;
        }

        public void Dispose()
        {
            if (this.stream != null)
                this.stream.Dispose();
        }

        public Task<Stream> GetStreamAsync()
        {
            return Task.FromResult(this.stream);
        }

        public Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            return Task.CompletedTask;
        }
    }


}