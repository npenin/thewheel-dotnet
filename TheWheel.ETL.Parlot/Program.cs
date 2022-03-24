using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Parlot;
using Parlot.Fluent;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Parlot
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var script = await File.ReadAllTextAsync(args[0]);
                FluentParser.TypeResolver.UsingAll(typeof(Providers.Csv).Assembly);
                FluentParser.TypeResolver.Register("Retry3", typeof(Providers.Retry3<,>));
                FluentParser.TypeResolver.Register("Convert", typeof(Convert));
                FluentParser.TypeResolver.Register("Encoding", typeof(System.Text.Encoding));
                FluentParser.TypeResolver.Register<SqlBulkCopyColumnMapping>("Mapping");
                FluentParser.TypeResolver.Register<System.Net.ICredentials>("ICredentials");
                FluentParser.TypeResolver.Register<System.Net.NetworkCredential>("NetworkCredential");
                FluentParser.TypeResolver.Register<TheWheel.ETL.Contracts.TreeOptions>("TreeOptions");
                FluentParser.TypeResolver.Register<TheWheel.ETL.Contracts.TreeLeaf>("TreeLeaf");
                FluentParser.TypeResolver.Register("PagedTransport", typeof(Providers.PagedTransport<,>));
                FluentParser.TypeResolver.Register("Parameter", typeof(KeyValuePair<string, object>));
                FluentParser.TypeResolver.Using("TheWheel.ETL.Providers");
                if (!FluentParser.Expression.TryParse(new Context(new Scanner(script)), out var result, out var error))
                {
                    if (error != null)
                    {
                        Console.WriteLine(error.Position);
                        Console.WriteLine(error.Message);
                    }
                    else
                        Console.WriteLine("Parsing failed");
                    return;
                }

                if (result != null)
                    ((Task)Expression.Lambda(result).Compile().DynamicInvoke()).Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                if (Debugger.IsAttached)
                    Debugger.Break();
            }
        }
    }
}
