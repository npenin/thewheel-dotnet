using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class OAuth : ITransport<Stream>, ITransport<HttpContent>
    {
        private List<KeyValuePair<string, object>> innerParameters;
        private ITransport<Stream> inner;
        private string innerConnectionString;

        public void Dispose()
        {

        }

        public async Task<ITransport<Stream>> GetStreamAsync(CancellationToken token)
        {
            await inner.InitializeAsync(innerConnectionString, token, innerParameters.ToArray());
            return inner;
        }

        public OAuth(ITransport<Stream> inner)
        {
            this.inner = inner;
        }

        public async Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            ITransport<HttpContent> auth = new Http();
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));
            var newParameters = new List<KeyValuePair<string, object>>(parameters.Length);
            innerParameters = new List<KeyValuePair<string, object>>(parameters.Length);
            foreach (var p in parameters)
            {
                if (p.Key == "Inner.ConnectionString")
                    this.innerConnectionString = (string)p.Value;
                else if (p.Key.StartsWith("Inner."))
                    innerParameters.Add(new KeyValuePair<string, object>(p.Key.Substring(5), p.Value));
                else
                    newParameters.Add(p);
            }
            await auth.InitializeAsync(connectionString, token, newParameters.ToArray());
            var content = await auth.GetStreamAsync(token);
            switch (content.Headers.ContentType.MediaType)
            {
                case "application/json":
                case "text/json":
                default:
#if NET5_0_OR_GREATER
                    var transport = Transport.From(await content.ReadAsStreamAsync(token));
#else
                    var transport = Transport.From(await content.ReadAsStreamAsync());
#endif

                    var reader = (await new Json().Configure(new TreeOptions { Transport = transport }.AddMatch("json://", "access_token/text()", "expires_in/text()", "refresh_token/text()"), token));
                    if (!reader.Read())
                        throw new InvalidDataException("Unable to get the oauth token");
                    var accessToken = reader.GetString(0);
                    // var expires_in = reader.GetInt32(0);
                    // var refresh_token = reader.GetString(0);
                    innerParameters.Add(new KeyValuePair<string, object>("_Authorization", "Bearer " + accessToken));
                    break;
            }
        }

        Task<Stream> ITransport<Stream>.GetStreamAsync(CancellationToken token)
        {
            return ((ITransport<Stream>)inner).GetStreamAsync(token);
        }

        Task<HttpContent> ITransport<HttpContent>.GetStreamAsync(CancellationToken token)
        {
            return ((ITransport<HttpContent>)inner).GetStreamAsync(token);
        }
    }
}