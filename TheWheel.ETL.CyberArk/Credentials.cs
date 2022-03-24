using System;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.CyberArk
{
    public class Credentials
    {
        private readonly Uri endpoint;
        private readonly string applicationId;

        public Credentials(Uri endpoint, string applicationId)
        {
            this.endpoint = endpoint;
            this.applicationId = applicationId;
        }
        public Credentials(string endpoint, string applicationId)
        : this(new Uri(endpoint), applicationId)
        {
        }

        public async Task<ICredentials> Use(string userName, string safe)
        {
            var provider = await Json.From<Http>(endpoint.ToString(),
            new System.Collections.Generic.KeyValuePair<string, object>("safe", safe),
            new System.Collections.Generic.KeyValuePair<string, object>("appId", applicationId),
            new System.Collections.Generic.KeyValuePair<string, object>("UserName", userName));
            await provider.QueryAsync(new TreeOptions().AddMatch(
                "json:///",
                "Content/text()",
                "UserName/text()"
            ), System.Threading.CancellationToken.None);
            var reader = provider.ExecuteReader();

            return new NetworkCredential(reader.GetString(reader.GetOrdinal("json:///UserName/text()")), reader.GetString(reader.GetOrdinal("json:///Content/text()")));
        }


        public async Task<ICredentials> Use(string userName)
        {
            var provider = await Json.From<Http>(endpoint.ToString(),
            new System.Collections.Generic.KeyValuePair<string, object>("appId", applicationId),
            new System.Collections.Generic.KeyValuePair<string, object>("UserName", userName));
            await provider.QueryAsync(new TreeOptions().AddMatch(
                "json:///",
                "Content/text()",
                "UserName/text()"
            ), System.Threading.CancellationToken.None);
            var reader = provider.ExecuteReader();
            if (reader.Read())
                return new NetworkCredential(reader.GetString(reader.GetOrdinal("json:///UserName/text()")), reader.GetString(reader.GetOrdinal("json:///Content/text()")));
            return null;
        }
    }
}
