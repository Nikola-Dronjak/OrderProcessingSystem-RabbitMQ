using Common.Events;
using Common.Messaging;
using Common.Messaging.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace InventoryModule.Consumers
{
    public class OrderCreatedConsumer : BackgroundService
    {
        private const string QueueName = "inventory.order-created";
        private const string RoutingKey = "order-created";

        private readonly RabbitMQSettings settings;
        private readonly IMessageBus messageBus;
        private IConnection connection;
        private IChannel channel;
        private readonly ILogger<OrderCreatedConsumer> logger;

        public OrderCreatedConsumer(IOptions<RabbitMQSettings> options, IMessageBus messageBus, ILogger<OrderCreatedConsumer> logger)
        {
            this.settings = options.Value;
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = this.settings.HostName,
                Port = this.settings.Port,
                UserName = this.settings.Username,
                Password = this.settings.Password
            };

            this.connection = factory.CreateConnectionAsync().Result;
            this.channel = this.connection.CreateChannelAsync().Result;

            await this.channel.ExchangeDeclareAsync(
                exchange: RabbitMQConstants.ExchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await this.channel.QueueDeclareAsync(
                queue: QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await this.channel.QueueBindAsync(
                queue: QueueName,
                exchange: RabbitMQConstants.ExchangeName,
                routingKey: RoutingKey,
                cancellationToken: stoppingToken);

            AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(this.channel);
            consumer.ReceivedAsync += async (sender, eventArgs) =>
            {
                try
                {
                    string json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", json);

                    OrderCreatedEvent? orderCreated = JsonSerializer.Deserialize<OrderCreatedEvent>(json);
                    if (orderCreated == null)
                        return;

                    // Simulate inventory processing logic
                    await Task.Delay(Random.Shared.Next(500, 2000), stoppingToken);

                    InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent
                    {
                        OrderId = orderCreated.OrderId,
                        ProductId = orderCreated.ProductId,
                        Quantity = orderCreated.Quantity,
                        CorrelationId = orderCreated.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        routingKey: "inventory-reserved",
                        message: inventoryReservedEvent,
                        cancellationToken: stoppingToken);

                    await this.channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation("Inventory reserved for order {OrderId}", orderCreated.OrderId);
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Error processing message");

                    if (this.channel != null)
                    {
                        await this.channel.BasicNackAsync(
                            deliveryTag: eventArgs.DeliveryTag,
                            multiple: false,
                            requeue: true,
                            cancellationToken: stoppingToken);
                    }
                }
            };

            await this.channel.BasicConsumeAsync(
                queue: "inventory.order-created",
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            this.logger.LogInformation("Inventory consumer started");

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        public override void Dispose()
        {
            this.channel?.Dispose();
            this.connection?.Dispose();

            base.Dispose();
        }
    }
}
