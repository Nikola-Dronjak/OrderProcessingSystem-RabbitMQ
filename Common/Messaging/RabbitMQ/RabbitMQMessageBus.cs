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
        private readonly RabbitMQSettings settings;
        private readonly ILogger<RabbitMQMessageBus> logger;

        public RabbitMQMessageBus(IOptions<RabbitMQSettings> options, ILogger<RabbitMQMessageBus> logger)
        {
            this.settings = options.Value;
            this.logger = logger;
            (this.connection, this.channel) = this.InitializeRabbitMQ();
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

        private (IConnection, IChannel) InitializeRabbitMQ()
        {
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = this.settings.HostName,
                Port = this.settings.Port,
                UserName = this.settings.Username,
                Password = this.settings.Password
            };

            const int maxRetries = 10;
            TimeSpan retryDelay = TimeSpan.FromSeconds(5);
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    this.logger.LogInformation(
                        """
                        Attempting RabbitMQ connection.
                        Attempt: {Attempt}
                        """,
                        attempt);

                    IConnection connection = factory.CreateConnectionAsync().Result;
                    IChannel channel = connection.CreateChannelAsync().Result;

                    this.logger.LogInformation("RabbitMQ connection established");

                    return (connection, channel);
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(
                        ex,
                        """
                        RabbitMQ connection attempt failed.
                        Attempt: {Attempt}
                        """,
                        attempt);

                    Thread.Sleep(retryDelay);
                }
            }
            throw new InvalidOperationException("Failed to establish RabbitMQ connection after maximum retry attempts.");
        }
    }
}
