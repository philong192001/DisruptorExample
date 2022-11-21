namespace TestDisruptor;

public class RingOutData
{
    public int UpdateRedisConsumerId { get; set; }
    public int PersistOracleByKafkaConsumerId { get; set; }
    public int CommandIndex { get; set; }
    public string ClientCode { get; set; }
    public string RequestId { get; set; }
}