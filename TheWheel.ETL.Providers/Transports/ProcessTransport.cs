using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{

    public abstract class ProcessTransport : ITransport<Stream>, IConfigurable<Process, ProcessTransport>
    {
        protected Process process;

        public ProcessTransport Configure(Process options)
        {
            this.process = options;
            return this;
        }

        public void Dispose()
        {
        }

        public abstract Task<Stream> GetStreamAsync();

        public Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            return Task.CompletedTask;
        }
    }



    public class StdOutput : ProcessTransport
    {

        public override Task<Stream> GetStreamAsync()
        {
            if (process == null)
                return Task.FromResult<Stream>(Console.OpenStandardOutput());
            return Task.FromResult<Stream>(process.StandardOutput.BaseStream);
        }
    }

    public class StdInput : ProcessTransport
    {
        public override Task<Stream> GetStreamAsync()
        {
            if (process == null)
                return Task.FromResult<Stream>(Console.OpenStandardInput());
            return Task.FromResult<Stream>(process.StandardInput.BaseStream);
        }
    }

    public class StdError : ProcessTransport
    {
        public override Task<Stream> GetStreamAsync()
        {
            if (process == null)
                return Task.FromResult<Stream>(Console.OpenStandardError());
            return Task.FromResult<Stream>(process.StandardError.BaseStream);
        }
    }


}