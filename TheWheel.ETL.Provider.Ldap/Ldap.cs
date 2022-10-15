
using System;
using System.Collections.Generic;
using System.Data;
using System.DirectoryServices.Protocols;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Provider.Ldap
{

    [ComImport]
    [Guid("9068270B-0939-11D1-8BE1-00C04FD8D503")]
    [InterfaceType(ComInterfaceType.InterfaceIsDual)]
    internal interface IADsLargeInteger
    {
        [DispId(0x00000002)]
        int HighPart { get; set; }
        [DispId(0x00000003)]
        int LowPart { get; set; }
    }

    public class Ldap : DataProvider<LdapReader, LdapOptions, LdapTransport>
    {
        public static async Task<IAsyncQueryable<LdapOptions>> From(string connectionString, CancellationToken token, NetworkCredential creds = null)
        {
            var transport = new LdapTransport();
            if (creds == null)
                await transport.InitializeAsync(connectionString, token);
            else
                await transport.InitializeAsync(connectionString, token, new KeyValuePair<string, object>("Credentials", creds));
            var result = new Ldap();
            result.Initialize(transport);
            return result;
        }
    }

    public class LdapOptions : IConfigurableAsync<LdapTransport, LdapOptions>, ITransportable<LdapTransport>
    {
        public LdapTransport Transport { get; set; }

        public SearchRequest Request { get; set; }

        public TimeSpan? Timeout { get; set; }

        public CancellationToken Token { get; set; }

        public Task<LdapOptions> Configure(LdapTransport transport, CancellationToken token)
        {
            return Task.FromResult(new LdapOptions { Transport = transport, Request = Request, Timeout = Timeout, Token = token });
        }
    }
}