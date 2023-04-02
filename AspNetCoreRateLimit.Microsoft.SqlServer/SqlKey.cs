namespace AspNetCoreRateLimit.Microsoft.SqlServer
{
    public class SqlKey
    {
        public string CounterId { get; private set; }

        public string Key { get; set; }
        public string Timeout { get; set; }
        public string Delta { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="counterId"></param>
        public SqlKey(string counterId)
        {
            CounterId = counterId;
        }
    }
}