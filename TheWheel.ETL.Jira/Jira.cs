using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
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

        async Task<Stream> ITransport<Stream>.GetStreamAsync()
        {
            var content = await this.GetStreamAsync();
            return await content.ReadAsStreamAsync();
        }
    }
}
