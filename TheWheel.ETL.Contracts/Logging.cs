using Microsoft.Extensions.Logging;

namespace TheWheel.ETL
{
    public class Logging
    {
        public static ILoggerFactory factory = LoggerFactory.Create((builder) => { });
    }
}