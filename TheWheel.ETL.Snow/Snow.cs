using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Snow
{
    public class Snow : PagedTransport<Http, HttpResponseMessage>, ITransport<Stream>
    {
        public Snow()
        : base("sysparm_offset", "sysparm_limit")
        {
        }

        async Task<Stream> ITransport<Stream>.GetStreamAsync(CancellationToken token)
        {
            var response = await this.GetStreamAsync(token);
            foreach (var value in response.Headers.GetValues("X-Total-Count"))
                Total = Convert.ToInt32(value);
#if NET5_0_OR_GREATER
            return await response.Content.ReadAsStreamAsync(token);
#else
            return await response.Content.ReadAsStreamAsync();
#endif
        }
    }
}
