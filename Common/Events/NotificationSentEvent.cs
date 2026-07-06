namespace Common.Events
{
    public class NotificationSentEvent : BaseEvent
    {
        public Guid OrderId { get; set; }
        public bool IsSuccessful { get; set; }
    }
}
