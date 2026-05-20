using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace Common.Messaging.RabbitMQ
{
    public class RabbitMQMessageBus : IMessageBus
    {
        private readonly IConnection connection;
        private readonly IChannel channel;
        private readonly ILogger<RabbitMQMessageBus> logger;

        public RabbitMQMessageBus(IOptions<RabbitMQSettings> options, ILogger<RabbitMQMessageBus> logger)
        {
            RabbitMQSettings settings = options.Value;

            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = settings.HostName,
                Port = settings.Port,
                UserName = settings.Username,
                Password = settings.Password
            };

            this.connection = factory.CreateConnectionAsync().Result;
            this.channel = this.connection.CreateChannelAsync().Result;

            this.logger = logger;
        }

        public async Task PublishAsync<T>(string routingKey, T message, CancellationToken cancellationToken)
        {
            await this.channel.ExchangeDeclareAsync(
                exchange: RabbitMQConstants.ExchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            string payload = JsonSerializer.Serialize(message);
            byte[] body = Encoding.UTF8.GetBytes(payload);
            BasicProperties properties = new BasicProperties
            {
                Persistent = true
            };

            await this.channel.BasicPublishAsync(
                exchange: RabbitMQConstants.ExchangeName,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);

            this.logger.LogInformation("""
                MESSAGE PUBLISHED
                Exchange: {Exchange}
                RoutingKey: {RoutingKey}
                Payload: {Payload}
                """,
                RabbitMQConstants.ExchangeName,
                routingKey,
                payload);
        }

        public void Dispose()
        {
            this.channel?.Dispose();
            this.connection?.Dispose();
        }
    }
}
