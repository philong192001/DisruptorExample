using Disruptor;

namespace TestDisruptor;

public class PersistOracleByKafkaConsumer : IEventHandler<RingOutData>
{
    private readonly int _id;

    public PersistOracleByKafkaConsumer(int id)
    {
        _id = id;
    }
    public void OnEvent(RingOutData data, long sequence, bool endOfBatch)
    {
        /// Chỉ xử lý khi _id của Consumer này bằng LogConsumerId từ ringData
        if (data.PersistOracleByKafkaConsumerId == _id)
        {
            Console.WriteLine($"-------- (C4) {data.ClientCode} {data.RequestId} - PERSIST ORACLE BY KAFKA handler for command {data.CommandIndex}");
            Thread.Sleep(50);
        }
    }
}