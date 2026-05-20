namespace Common.Messaging
{
    public interface IMessageBus
    {
        Task PublishAsync<T>(string routingKey, T message, CancellationToken cancellationToken);
    }
}
