namespace AspNetCoreRateLimit.Microsoft.SqlServer
{
    public class SqlCacheItem
    {
        public SqlKey Key { get; set; }
        public double Timeout { get; set; }
        public double Delta { get; set; }
    }
}