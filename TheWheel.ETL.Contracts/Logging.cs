using Microsoft.Extensions.Logging;

namespace TheWheel.ETL
{
    public class Logging
    {
        public readonly static ILoggerFactory factory = LoggerFactory.Create((builder) => { });
    }
}