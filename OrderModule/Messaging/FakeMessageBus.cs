using System.Text.Json;

namespace OrderModule.Messaging
{
    public class FakeMessageBus : IMessageBus
    {
        private readonly ILogger<FakeMessageBus> _logger;

        public FakeMessageBus(ILogger<FakeMessageBus> logger)
        {
            _logger = logger;
        }

        public Task PublishAsync<T>(
            string topic,
            T message,
            CancellationToken cancellationToken)
        {
            var payload = JsonSerializer.Serialize(message);

            _logger.LogInformation("""
                MESSAGE PUBLISHED
                Topic: {Topic}
                Payload: {Payload}
                """,
                topic,
                payload);

            return Task.CompletedTask;
        }
    }
}
