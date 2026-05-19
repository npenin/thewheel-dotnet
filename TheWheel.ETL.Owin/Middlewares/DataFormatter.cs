using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;

namespace TheWheel.ETL.Owin
{
    public interface IDataFormatter
    {
        Task FormatAsync(IDataProvider data, HttpContext context);
        Task FormatAsync(IDataProvider data, Stream output, string accept, CancellationToken cancellationToken);
    }

    public class DataFormatter : IDataFormatter
    {
        private static readonly Dictionary<string, Func<IDataProvider, Stream, CancellationToken, Task>> Formatters = new();

        static DataFormatter()
        {
            AddJsonFormatter();
        }

        public static void AddJsonFormatter()
        {
            RegisterFormatter<Json, TreeOptions>("application/json", () => new TreeOptions().AddMatch("json:///"));
            RegisterFormatter<Json, TreeOptions>("text/json", () => new TreeOptions().AddMatch("json:///"));
        }

        public static void AddCsvFormatter()
        {
            RegisterFormatter<Csv, CsvReceiverOptions>("text/csv");
        }

        public static void RegisterFormatter<T, TOptions>(string mediaType)
            where T : IDataReceiver<TOptions>, new()
            where TOptions : IConfigurableAsync<ITransport<Stream>, TOptions>, new()
        {
            RegisterFormatter<T, TOptions>(mediaType, () => new TOptions());
        }

        public static void RegisterFormatter<T, TOptions>(string mediaType, Func<TOptions> optionsFactory)
            where T : IDataReceiver<TOptions>, new()
            where TOptions : IConfigurableAsync<ITransport<Stream>, TOptions>
        {
            Formatters[mediaType] = async (provider, stream, token) =>
            {
                var receiver = new T();
                var options = optionsFactory();
                options = await options.Configure(new StreamTransport().Configure(stream), token);
                await receiver.ReceiveAsync(provider, options, token);
            };
        }

        public Task FormatAsync(IDataProvider data, HttpContext context)
        {
            var accepts = context.Request.Headers.GetCommaSeparatedValues("Accept")
                .Select(h => MediaTypeWithQualityHeaderValue.TryParse(h, out var accept) ? accept : null)
                .Where(h => h != null)
                .OrderByDescending(h => h.Quality);

            foreach (var accept in accepts)
            {
                if (Formatters.TryGetValue(accept.MediaType, out var formatter))
                {
                    context.Response.ContentType = accept.MediaType;
                    return formatter(data, context.Response.Body, context.RequestAborted);
                }
            }

            // Default to JSON
            context.Response.ContentType = "application/json";
            return FormatAsJson(data, context.Response.Body, context.RequestAborted);
        }

        public Task FormatAsync(IDataProvider data, Stream output, string accept, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrEmpty(accept) && Formatters.TryGetValue(accept, out var formatter))
                return formatter(data, output, cancellationToken);

            return FormatAsJson(data, output, cancellationToken);
        }

        private static Task FormatAsJson(IDataProvider data, Stream output, CancellationToken cancellationToken)
        {
            var json = new Json();
            return json.ReceiveAsync(data, new TreeOptions { Transport = new StreamTransport().Configure(output) }.AddMatch("json:///"), cancellationToken);
        }
    }
}
