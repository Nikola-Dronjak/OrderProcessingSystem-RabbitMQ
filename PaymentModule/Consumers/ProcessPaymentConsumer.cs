using Common.Commands;
using Common.Events;
using Common.Messaging;
using Common.Messaging.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections;
using System.Text;
using System.Text.Json;

namespace PaymentModule.Consumers
{
    public class ProcessPaymentConsumer : BackgroundService
    {
        #region Constants
        private const string PaymentQueueName = "payment.process-payment";
        private const string RetryQueueName = "payment.retry-queue";
        private const string DeadLetterQueueName = "payment.dead-letter-queue";
        private const int RetryDelayMilliseconds = 5000;
        private const int SimulatedPaymentProcessingDelayMilliseconds = 2000;
        private const int MaxRetries = 3;
        #endregion

        private readonly RabbitMQSettings settings;
        private readonly IMessageBus messageBus;
        private readonly IConfiguration configuration;
        private IConnection connection;
        private IChannel channel;
        private readonly ILogger<ProcessPaymentConsumer> logger;

        public ProcessPaymentConsumer(IOptions<RabbitMQSettings> options, IMessageBus messageBus, IConfiguration configuration, ILogger<ProcessPaymentConsumer> logger)
        {
            this.settings = options.Value;
            this.messageBus = messageBus;
            this.configuration = configuration;
            this.logger = logger;
        }

        #region Public methods
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

            #region Main Payment Queue
            await this.channel.QueueDeclareAsync(
                queue: PaymentQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-dead-letter-exchange", RabbitMQConstants.ExchangeName },
                    { "x-dead-letter-routing-key", RabbitMQConstants.PaymentFailedRoutingKey }
                },
                cancellationToken: stoppingToken);

            await this.channel.QueueBindAsync(
                queue: PaymentQueueName,
                exchange: RabbitMQConstants.ExchangeName,
                routingKey: RabbitMQConstants.ProcessPaymentRoutingKey,
                cancellationToken: stoppingToken);
            #endregion

            #region Retry Queue
            await this.channel.QueueDeclareAsync(
                queue: RetryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    { "x-message-ttl", RetryDelayMilliseconds },
                    { "x-dead-letter-exchange", RabbitMQConstants.ExchangeName },
                    { "x-dead-letter-routing-key", RabbitMQConstants.ProcessPaymentRoutingKey }
                },
                cancellationToken: stoppingToken);

            await this.channel.QueueBindAsync(
                queue: RetryQueueName,
                exchange: RabbitMQConstants.ExchangeName,
                routingKey: RabbitMQConstants.PaymentFailedRoutingKey,
                cancellationToken: stoppingToken);
            #endregion

            #region Dead Letter Queue
            await this.channel.QueueDeclareAsync(
                queue: DeadLetterQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stoppingToken);

            await this.channel.QueueBindAsync(
                queue: DeadLetterQueueName,
                exchange: RabbitMQConstants.ExchangeName,
                routingKey: RabbitMQConstants.PaymentDeadLetterRoutingKey,
                cancellationToken: stoppingToken);
            #endregion

            AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(this.channel);
            consumer.ReceivedAsync += async (sender, eventArgs) =>
            {
                byte[] body = eventArgs.Body.ToArray();
                string json = Encoding.UTF8.GetString(body);

                this.logger.LogInformation("MESSAGE RECEIVED: {Message}", json);

                ProcessPaymentCommand? processPaymentCommand = JsonSerializer.Deserialize<ProcessPaymentCommand>(json);
                if (processPaymentCommand == null)
                    return;

                int retryCount = this.GetRetryCount(eventArgs.BasicProperties);
                try
                {
                    // Simulate payment processing logic
                    await Task.Delay(SimulatedPaymentProcessingDelayMilliseconds, stoppingToken);

                    // Simulate random failure
                    int paymentFailurePercentage = this.configuration.GetValue<int>("PaymentProcessing:FailurePercentage");
                    if (Random.Shared.Next(1, 101) <= paymentFailurePercentage)
                        throw new Exception("Simulated payment processing failure.");

                    // Payment succeeded, publish success event
                    PaymentSucceededEvent paymentSucceededEvent = new PaymentSucceededEvent
                    {
                        OrderId = processPaymentCommand.OrderId,
                        Price = processPaymentCommand.Price,
                        PaymentId = Guid.NewGuid(),
                        CorrelationId = processPaymentCommand.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        routingKey: RabbitMQConstants.PaymentSucceededRoutingKey,
                        message: paymentSucceededEvent,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation("Payment processed for order {OrderId}", processPaymentCommand.OrderId);

                    await this.channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(
                        ex,
                        """
                        PAYMENT FAILED
                        RetryCount: {RetryCount}
                        OrderId: {OrderId}
                        """,
                        retryCount,
                        processPaymentCommand.OrderId);

                    if (retryCount < MaxRetries)
                    {
                        this.logger.LogWarning(
                            """
                            MESSAGE SENT TO RETRY QUEUE
                            RetryAttempt: {RetryAttempt}
                            OrderId: {OrderId}
                            """,
                            retryCount + 1,
                            processPaymentCommand.OrderId);

                        // Reject message
                        // RabbitMQ sends it to retry queue
                        await this.channel.BasicNackAsync(
                            deliveryTag: eventArgs.DeliveryTag,
                            multiple: false,
                            requeue: false,
                            cancellationToken: stoppingToken);

                        return;
                    }

                    NotificationSentEvent notificationSentEvent = new NotificationSentEvent
                    {
                        OrderId = processPaymentCommand.OrderId,
                        IsSuccessful = false,
                        CorrelationId = processPaymentCommand.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    // Publish notification sent event to indicate that the message has been sent to the dead-letter queue
                    await messageBus.PublishAsync(
                        routingKey: RabbitMQConstants.NotificationSentRoutingKey,
                        message: notificationSentEvent,
                        cancellationToken: stoppingToken);

                    // Exceeded retries -> publish to explicit dead-letter queue
                    await this.PublishToDeadLetterQueueAsync(
                        body: body,
                        sourceProperties: eventArgs.BasicProperties,
                        cancellationToken: stoppingToken);

                    await this.channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);

                    this.logger.LogError(
                        """
                        MESSAGE SENT TO DEAD LETTER QUEUE
                        OrderId: {OrderId}
                        RetryCount: {RetryCount}
                        """,
                        processPaymentCommand.OrderId,
                        retryCount);
                }
            };

            await this.channel.BasicConsumeAsync(
                queue: PaymentQueueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            this.logger.LogInformation("Payment consumer started");
        }

        public override void Dispose()
        {
            this.channel?.Dispose();
            this.connection?.Dispose();

            base.Dispose();
        }
        #endregion

        #region Private methods
        private int GetRetryCount(IReadOnlyBasicProperties basicProperties)
        {
            if (basicProperties?.Headers == null ||
                !basicProperties.Headers.TryGetValue("x-death", out object? deathHeader) ||
                !(deathHeader is IEnumerable deathsEnumerable))
            {
                return 0;
            }

            int retryCount = 0;
            foreach (object? deathEntry in deathsEnumerable)
            {
                if (deathEntry is not IDictionary<string, object?> deathInfo)
                    continue;

                if (!deathInfo.TryGetValue("count", out object? countObj) || countObj == null)
                    continue;

                long count = this.ConvertToLong(countObj);
                string? queueName = this.ExtractQueueName(deathInfo);

                if (string.Equals(queueName, PaymentQueueName, StringComparison.OrdinalIgnoreCase))
                {
                    return (int)count;
                }

                retryCount += (int)count;
            }

            return retryCount;
        }

        private long ConvertToLong(object value)
        {
            return value switch
            {
                long l => l,
                int i => i,
                byte b => b,
                _ => 0
            };
        }

        private string? ExtractQueueName(IDictionary<string, object?> deathInfo)
        {
            if (!deathInfo.TryGetValue("queue", out object? queueObject) || queueObject == null)
                return null;

            return queueObject is byte[] qBytes ? Encoding.UTF8.GetString(qBytes) : queueObject.ToString();
        }

        private async Task PublishToDeadLetterQueueAsync(byte[] body, IReadOnlyBasicProperties sourceProperties, CancellationToken cancellationToken)
        {
            try
            {
                BasicProperties props = new BasicProperties
                {
                    Persistent = true,
                    ContentType = sourceProperties?.ContentType,
                    CorrelationId = sourceProperties?.CorrelationId,
                    Type = sourceProperties?.Type
                };

                if (sourceProperties?.Headers != null)
                {
                    props.Headers = new Dictionary<string, object?>(sourceProperties.Headers);
                }

                await this.channel.BasicPublishAsync(
                    exchange: RabbitMQConstants.ExchangeName,
                    routingKey: RabbitMQConstants.PaymentDeadLetterRoutingKey,
                    mandatory: false,
                    basicProperties: props,
                    body: body,
                    cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed publishing message to dead letter queue");
            }
        }
        #endregion
    }
}
