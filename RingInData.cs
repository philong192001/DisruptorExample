namespace TestDisruptor;

public class RingInData
{
    public int LogConsumerId { get; set; }
    public int RedisConsumerId { get; set; }
    public int CommandIndex { get; set; }
    public string ClientCode { get; set; }
    public string RequestId { get; set; }
}