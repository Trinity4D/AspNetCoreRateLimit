using Microsoft.Extensions.DependencyInjection;

namespace AspNetCoreRateLimit.Microsoft.SqlServer
{
    public static class StartupExtensions
    {
        public static IServiceCollection AddSqlServerRateLimiting(this IServiceCollection services)
        {
            services.AddDistributedRateLimiting<SqlServerProcessingStrategy>();
            return services;
        }
    }
}