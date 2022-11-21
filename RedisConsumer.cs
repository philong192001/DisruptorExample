using System.Reflection.Metadata.Ecma335;
using Disruptor;

namespace TestDisruptor;

public class RedisConsumer : IEventHandler<RingInData>
{
    private readonly int _id;

    public RedisConsumer(int id)
    {
        _id = id;
    }
    public void OnEvent(RingInData data, long sequence, bool endOfBatch)
    {
        /// Chỉ xử lý khi _id của Consumer này bằng RedisConsumerId từ ringData
        if (data.RedisConsumerId == _id)
        {
            Console.WriteLine($"(C1) {data.ClientCode} {data.RequestId} - RECOVER REDIS handler for command {data.CommandIndex}");
            Thread.Sleep(200);
        }
    }
}