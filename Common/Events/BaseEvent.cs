namespace Common.Events
{
    public abstract class BaseEvent
    {
        public string CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
