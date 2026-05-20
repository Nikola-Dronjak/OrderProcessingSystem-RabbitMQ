using Common.Messaging.RabbitMQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Common.Messaging.Extensions
{
    public static class ServiceCollectionExtension
    {
        public static IServiceCollection AddRabbitMQMessaging(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<RabbitMQSettings>(configuration.GetSection("RabbitMQ"));
            services.AddSingleton<IMessageBus, RabbitMQMessageBus>();
            return services;
        }
    }
}
