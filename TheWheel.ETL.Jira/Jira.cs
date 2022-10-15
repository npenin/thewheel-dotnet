using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Jira
{
    public class Jira : PagedTransport<Http, HttpContent>, ITransport<Stream>
    {
        public Jira()
        : base("startAt", "maxResults")
        {
        }

        async Task<Stream> ITransport<Stream>.GetStreamAsync(CancellationToken token)
        {
            var content = await this.GetStreamAsync(token);
#if NET5_0_OR_GREATER
            return await content.ReadAsStreamAsync(token);
#else
            return await content.ReadAsStreamAsync();
#endif
        }
    }
}
