using Microsoft.Extensions.Options;
using OrderModule.Configuration;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace OrderModule.Messaging
{
    public class RabbitMQMessageBus : IMessageBus
    {
        private const string ExchangeName = "order-processing";

        private readonly RabbitMQSettings settings;
        private readonly IConnection connection;
        private readonly IChannel channel;
        private readonly ILogger<RabbitMQMessageBus> logger;

        public RabbitMQMessageBus(IOptions<RabbitMQSettings> options, ILogger<RabbitMQMessageBus> logger)
        {
            this.settings = options.Value;

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

        public async Task PublishAsync<T>(string topic, T message, CancellationToken cancellationToken)
        {
            await this.channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
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
                exchange: ExchangeName,
                routingKey: topic,
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
                ExchangeName,
                topic,
                payload);
        }

        public void Dispose()
        {
            this.channel?.Dispose();
            this.connection?.Dispose();
        }
    }
}
