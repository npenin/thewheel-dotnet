using System.Threading.Tasks;
using System.Collections.Generic;
using System.DirectoryServices.Protocols;
using System.Linq;
using System.Net;
using System.Threading;

namespace TheWheel.ETL.Provider.Ldap
{
    public class LdapTransport : Contracts.ITransport<LdapConnection>
    {
        private LdapConnection ldapConnection;

        public async Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            var id = new LdapDirectoryIdentifier(connectionString, true, false);
            if (parameters != null)
            {
                var creds = parameters.FirstOrDefault(p => p.Key == "Credentials").Value as NetworkCredential;
                if (creds != null)
                {
                    var authType = (AuthType)parameters.FirstOrDefault(p => p.Key == "AuthType").Value;
                    if (authType == AuthType.Anonymous)
                        this.ldapConnection = new LdapConnection(id, creds);
                    else
                        this.ldapConnection = new LdapConnection(id, creds, authType);
                }
            }
            if (this.ldapConnection == null)
                this.ldapConnection = new LdapConnection(id);

            ldapConnection.SessionOptions.ProtocolVersion = 3;

            await Task.Run(this.ldapConnection.Bind, token);
        }

        public Task<LdapConnection> GetStreamAsync(CancellationToken token)
        {
            return Task.FromResult(this.ldapConnection);
        }

        public void Dispose()
        {
            if (this.ldapConnection != null)
                this.ldapConnection.Dispose();
        }
    }
}