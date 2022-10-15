using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class Http : ITransport<Stream>, ITransport<HttpContent>, ITransport<HttpResponseMessage>
    {
        private HttpClient client;
        private TimeSpan timeout;
        protected Task<HttpResponseMessage> query;

        public Http(TimeSpan timeout)
        {
            this.timeout = timeout;
        }

        public Http()
        {

        }

        public void Dispose()
        {
        }

        public async Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            if (parameters != null && parameters.Length > 0)
            {
                var credentials = parameters.FirstOrDefault(p => p.Key == "Credentials");
                if (credentials.Key != null)
                {
                    ICredentials cred;
                    if (credentials.Value is Task<ICredentials> t)
                        cred = await t;
                    else
                        cred = (ICredentials)credentials.Value;
                    var credentialsScheme = parameters.FirstOrDefault(p => p.Key == "CredentialsScheme");
                    NetworkCredential netcred = cred as NetworkCredential;
                    if (netcred == null && credentialsScheme.Key != null)
                    {
                        netcred = cred.GetCredential(new Uri(connectionString), (string)credentialsScheme.Value);
                    }

                    if ((string)credentialsScheme.Value == "Basic")
                    {
                        client = new HttpClient();
                        client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes(netcred.UserName + ":" + netcred.Password)));
                    }
                    else
                        client = new HttpClient(new HttpClientHandler { Credentials = cred });
                }
            }
            if (client == null)
                client = new HttpClient();


            if (this.timeout != default(TimeSpan))
                client.Timeout = this.timeout;
            if (parameters != null && parameters.Length > 0)
            {
                var timeout = parameters.FirstOrDefault(p => p.Key == "Timeout");
                if (timeout.Key != null)
                    client.Timeout = (TimeSpan)timeout.Value;
                var url = new UriBuilder(connectionString);
                var queryString = QueryHelpers.ParseQuery(url.Query);
                url.Query = null;
                foreach (var parameter in parameters)
                {
                    if (parameter.Key[0] == '_')
                        client.DefaultRequestHeaders.TryAddWithoutValidation(parameter.Key.Substring(1), parameter.Value.ToString());
                    else if (parameter.Key != "Timeout" && parameter.Key != "Method" && parameter.Key != "Credentials")
                        queryString.Add(parameter.Key, Convert.ToString(parameter.Value));
                }
                var sb = new StringBuilder();
                foreach (var parameter in queryString)
                {
                    foreach (var value in parameter.Value)
                    {
                        if (sb.Length > 0)
                            sb.Append('&');
                        sb.Append(Uri.EscapeDataString(parameter.Key));
                        sb.Append('=');
                        sb.Append(Uri.EscapeDataString(value));
                    }
                }
                url.Query = sb.ToString();

                var method = parameters.FirstOrDefault(p => p.Key == "Method");
                if (method.Key != null)
                {
                    switch (method.Value.ToString().ToLower())
                    {
                        case "get":
                            query = client.GetAsync(url.Uri, token);
                            break;
                        case "post":
                            query = client.PostAsync(url.Uri, (HttpContent)parameters.First(p => p.Key == "Body").Value, token);
                            break;
                        case "put":
                            query = client.PutAsync(url.Uri, (HttpContent)parameters.First(p => p.Key == "Body").Value, token);
                            break;
#if NET5_0
                        case "patch":
                            query = client.PatchAsync(url.Uri, (HttpContent)parameters.First(p => p.Key == "Body").Value, token);
                            break;
#endif
                        default:
                            throw new ArgumentOutOfRangeException("Method", $"The method {method} is not supported");
                    }
                    await query;
                    return;
                }
                connectionString = url.ToString();
            }

            await (query = client.GetAsync(connectionString, token));

        }

        public async Task<Stream> GetStreamAsync(CancellationToken token)
        {
#if NET5_0_OR_GREATER
            return await (await ((ITransport<HttpContent>)this).GetStreamAsync(token)).ReadAsStreamAsync(token);
#else
            return await (await ((ITransport<HttpContent>)this).GetStreamAsync(token)).ReadAsStreamAsync();
#endif
        }

        async Task<HttpContent> ITransport<HttpContent>.GetStreamAsync(CancellationToken token)
        {
            return (await ((ITransport<HttpResponseMessage>)this).GetStreamAsync(token)).Content;
        }

        async Task<HttpResponseMessage> ITransport<HttpResponseMessage>.GetStreamAsync(CancellationToken token)
        {
            var response = await query;
            try
            {
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
#if NET5_0_OR_GREATER
                    System.Diagnostics.Debugger.Log((int)response.StatusCode, "Http", await response.Content.ReadAsStringAsync(token));
#else
                    System.Diagnostics.Debugger.Log((int)response.StatusCode, "Http", await response.Content.ReadAsStringAsync());
#endif
                }
                else
#if NET5_0_OR_GREATER
                    Console.Error.WriteLine(await response.Content.ReadAsStringAsync(token));
#else
                    Console.Error.WriteLine(await response.Content.ReadAsStringAsync());
#endif
                throw;
            }
            return response;
        }
    }
}
