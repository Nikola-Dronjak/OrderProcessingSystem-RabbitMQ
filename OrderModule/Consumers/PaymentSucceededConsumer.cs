using Common.Events;
using Common.Messaging;
using Common.Messaging.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace OrderModule.Consumers
{
    public class PaymentSucceededConsumer : BackgroundService
    {
        private const string QueueName = "order.payment-succeeded";

        private readonly RabbitMQSettings settings;
        private readonly IMessageBus messageBus;
        private IConnection connection;
        private IChannel channel;
        private readonly ILogger<PaymentSucceededConsumer> logger;

        public PaymentSucceededConsumer(IOptions<RabbitMQSettings> options, IMessageBus messageBus, ILogger<PaymentSucceededConsumer> logger)
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
                routingKey: RabbitMQConstants.PaymentSucceededRoutingKey,
                cancellationToken: stoppingToken);

            AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(this.channel);
            consumer.ReceivedAsync += async (sender, eventArgs) =>
            {
                try
                {
                    string json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", json);

                    PaymentSucceededEvent? paymentSucceededEvent = JsonSerializer.Deserialize<PaymentSucceededEvent>(json);
                    if (paymentSucceededEvent == null)
                        return;

                    OrderCompletedEvent orderCompletedEvent = new OrderCompletedEvent
                    {
                        OrderId = paymentSucceededEvent.OrderId,
                        Price = paymentSucceededEvent.Price,
                        PaymentId = paymentSucceededEvent.PaymentId,
                        CorrelationId = paymentSucceededEvent.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        routingKey: RabbitMQConstants.OrderCompletedRoutingKey,
                        message: orderCompletedEvent,
                        cancellationToken: stoppingToken);

                    await this.channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation("""
                        ORDER COMPLETED
                        OrderId: {OrderId}
                        CorrelationId: {CorrelationId}
                        """,
                        orderCompletedEvent.OrderId,
                        orderCompletedEvent.CorrelationId);
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
                queue: QueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            this.logger.LogInformation("PaymentSucceeded consumer started");

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
