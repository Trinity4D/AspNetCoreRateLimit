using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Data;

namespace AspNetCoreRateLimit.Microsoft.SqlServer
{
    public class RedisProcessingStrategy : ProcessingStrategy
    {
        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly IRateLimitConfiguration _config;
        private readonly ILogger<RedisProcessingStrategy> _logger;

        public RedisProcessingStrategy(IConnectionMultiplexer connectionMultiplexer, IRateLimitConfiguration config, ILogger<RedisProcessingStrategy> logger)
            : base(config)
        {
            _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentException("IConnectionMultiplexer was null. Ensure StackExchange.Redis was successfully registered");
            _config = config;
            _logger = logger;
        }

        static private readonly LuaScript _atomicIncrement = LuaScript.Prepare("local count = redis.call(\"INCRBYFLOAT\", @key, tonumber(@delta)) local ttl = redis.call(\"TTL\", @key) if ttl == -1 then redis.call(\"EXPIRE\", @key, @timeout) end return count");

        public override async Task<RateLimitCounter> ProcessRequestAsync(ClientRequestIdentity requestIdentity, RateLimitRule rule, ICounterKeyBuilder counterKeyBuilder, RateLimitOptions rateLimitOptions, CancellationToken cancellationToken = default)
        {
            var counterId = BuildCounterKey(requestIdentity, rule, counterKeyBuilder, rateLimitOptions);
            return await IncrementAsync(counterId, rule.PeriodTimespan.Value, _config.RateIncrementer);
        }

        public async Task<RateLimitCounter> IncrementAsync(string counterId, TimeSpan interval, Func<double> RateIncrementer = null)
        {
            var now = DateTime.UtcNow;
            var numberOfIntervals = now.Ticks / interval.Ticks;
            var intervalStart = new DateTime(numberOfIntervals * interval.Ticks, DateTimeKind.Utc);

            _logger.LogDebug("Calling Lua script. {counterId}, {timeout}, {delta}", counterId, interval.TotalSeconds, 1D);
            var count = await _connectionMultiplexer.GetDatabase().ScriptEvaluateAsync(_atomicIncrement, 
                new 
                { 
                    key     = new RedisKey(counterId), 
                    timeout = interval.TotalSeconds, 
                    delta   = RateIncrementer?.Invoke() ?? 1D
                });

            return new RateLimitCounter
            {
                Count = (double)count,
                Timestamp = intervalStart
            };
        }
    }
    public static class StartupExtensions
    {
        public static IServiceCollection AddSqlServerRateLimiting(this IServiceCollection services)
        {
            services.AddDistributedRateLimiting<SqlServerProcessingStrategy>();
            return services;
        }
    }

   

public class SqlKey
{
    public string CounterId { get; private set; }

     public string Key { get; set; }
        public string Timeout { get; set; }
        public string Delta { get; set; }

        public SqlKey(string counterId)
    {
        CounterId = counterId;
    }
}
    public class SqlCacheItem
    {
        public SqlKey Key { get; set; }
        public double Timeout { get; set; }
        public double Delta { get; set; }
    }
    public class SqlServerProcessingStrategy : ProcessingStrategy
    {
        private readonly SqlConnection _connection;
        private readonly IRateLimitConfiguration _config;
        private readonly ILogger<SqlServerProcessingStrategy> _logger;


        public SqlServerProcessingStrategy(IConfiguration configuration, IRateLimitConfiguration config, ILogger<SqlServerProcessingStrategy> logger):base(config)
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