using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Data;

namespace AspNetCoreRateLimit.Microsoft.SqlServer
{
    public class SqlServerProcessingStrategy : ProcessingStrategy
    {
        private readonly SqlConnection _connection;
        private readonly IRateLimitConfiguration _config;
        private readonly ILogger<SqlServerProcessingStrategy> _logger;

        /// <summary>
        ///  SqlServerProcessingStrategy constructor 
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        public SqlServerProcessingStrategy(
            IConfiguration          configuration, 
            IRateLimitConfiguration config, 
            ILogger<SqlServerProcessingStrategy> logger)
        : base(config)
        {
            //get connection string from appsettings.json
            var connectionString = configuration.GetConnectionString("DefaultConnection");

            //create connection
            _connection = new SqlConnection(connectionString);
            
            _config = config;
            _logger = logger;

        }


        //override ProcessRequestAsync
        public override async Task<RateLimitCounter> ProcessRequestAsync(ClientRequestIdentity requestIdentity, RateLimitRule rule, ICounterKeyBuilder counterKeyBuilder, RateLimitOptions rateLimitOptions, CancellationToken cancellationToken = default)
        {
            //implement
            if (requestIdentity == null)
            {
                throw new ArgumentNullException(nameof(requestIdentity));

            }
            if (rule == null)
                throw new ArgumentNullException(nameof(rule));
            if (counterKeyBuilder == null)
                throw new ArgumentNullException(nameof(counterKeyBuilder));
            if (rateLimitOptions == null)
                throw new ArgumentNullException(nameof(rateLimitOptions));

            var counterId = BuildCounterKey(requestIdentity, rule, counterKeyBuilder, rateLimitOptions);

            return await IncrementAsync(counterId, rule.PeriodTimespan.Value, _config.RateIncrementer);


        }
        //override IncrementAsync using ado.net
        public async Task<RateLimitCounter> IncrementAsync(string counterId, TimeSpan interval, Func<double> RateIncrementer = null)
        {
            var now = DateTime.UtcNow;
            var numberOfIntervals = now.Ticks / interval.Ticks;
            var intervalStart = new DateTime(numberOfIntervals * interval.Ticks, DateTimeKind.Utc);

            var cacheItem = new SqlCacheItem
            {
                Key     = new SqlKey(counterId),
                Timeout = interval.TotalSeconds,
                Delta   = RateIncrementer?.Invoke() ?? 1D
            };

            var json  = JsonConvert.SerializeObject(cacheItem);

            _logger.LogDebug("Calling stored procedure. {counterId}, {json}, {timeout}, {delta}", counterId, json, interval.TotalSeconds, cacheItem.Delta);

            using (var command = _connection.CreateCommand())
            {
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "dbo.IncrementCounter";

                command.Parameters.Add(new SqlParameter("counterId", counterId));
                command.Parameters.Add(new SqlParameter("key ",      json));
                command.Parameters.Add(new SqlParameter("timeout",   interval.TotalSeconds));
                command.Parameters.Add(new SqlParameter("delta",     cacheItem.Delta));

                try
                {
                    await _connection.OpenAsync();
                    
                    var count = await command.ExecuteScalarAsync();

                    return new RateLimitCounter
                    {
                        Count     = (double)count,
                        Timestamp = intervalStart
                    };
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while incrementing counter");
                    throw;
                }
                finally
                {
                    _connection.Close();
                }
            }
        }
    }
}