using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{

    public class CsvConnectionStringBuilder : DbConnectionStringBuilder
    {
        public CsvConnectionStringBuilder()
        {

        }

        public CsvConnectionStringBuilder(string connectionString)
            : base()
        {
            ConnectionString = connectionString;
        }

        public bool HasHeader
        {
            get
            {
                //defaults to true
                return !this.ContainsKey("HasHeader") || System.Convert.ToBoolean(this["HasHeader"]);
            }
            set { this["HasHeader"] = value; }
        }

        public int? SkipLines
        {
            get
            {
                //defaults to true
                if (this.ContainsKey("SkipLines"))
                    return System.Convert.ToInt32(this["SkipLines"]);
                return null;
            }
            set { this["SkipLines"] = value; }
        }


        public Separator Separator
        {
            get
            {
                //defaults to true
                if (this.ContainsKey("Separator"))
                    return (Separator)Enum.Parse(typeof(Separator), (string)this["Separator"]);
                return Separator.Guess;
            }
            set { this["Separator"] = value; }
        }


        public bool Archive
        {
            get
            {
                //defaults to true
                if (this.ContainsKey("Archive"))
                    return bool.Parse((string)this["Archive"]);
                return true;
            }
            set { this["Separator"] = value; }
        }
    }


    public enum Separator : ushort
    {
        Guess = 0,
        Comma = ',',
        SemiColon = ';',
        Colon = ':',
        Pipe = '|'
    }

    public class CsvOptions : IConfigurableAsync<ITransport<Stream>, CsvOptions>, ITransportable<ITransport<Stream>>
    {
        public CsvOptions()
        {

        }

        public CsvOptions(CsvOptions options)
        : this(options.Transport, options)
        {
        }

        public CsvOptions(ITransport<Stream> transport, CsvOptions other)
        {
            this.Separator = other.Separator;
            this.SkipLines = other.SkipLines;
            this.FirstLineHeader = other.FirstLineHeader;
            this.Header = other.Header;
            this.BufferSize = other.BufferSize;
            this.Transport = transport;
        }
        public Separator Separator = Separator.Guess;
        public string[] SkipLines;
        public bool FirstLineHeader = true;
        public string[] Header;

        public int BufferSize = 2048;

        public ITransport<Stream> Transport { get; private set; }

        public Task<CsvOptions> Configure(ITransport<Stream> transport, CancellationToken token)
        {
            return Task.FromResult(new CsvOptions(transport, this));
        }
    }

}
