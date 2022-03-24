using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
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

        async Task<Stream> ITransport<Stream>.GetStreamAsync()
        {
            var response = await this.GetStreamAsync();
            foreach (var value in response.Headers.GetValues("X-Total-Count"))
                Total = Convert.ToInt32(value);
            return await response.Content.ReadAsStreamAsync();
        }
    }
}
