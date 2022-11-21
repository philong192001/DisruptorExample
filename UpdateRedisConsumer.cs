using Disruptor;

namespace TestDisruptor;

public class UpdateRedisConsumer : IEventHandler<RingOutData>
{
    private readonly int _id;

    public UpdateRedisConsumer(int id)
    {
        _id = id;
    }
    public void OnEvent(RingOutData data, long sequence, bool endOfBatch)
    {
        /// Chỉ xử lý khi _id của Consumer này bằng LogConsumerId từ ringData
        if (data.UpdateRedisConsumerId == _id)
        {
            Console.WriteLine($"------ (C3) {data.ClientCode} {data.RequestId} - UPDATE REDIS handler for command {data.CommandIndex}");
            Thread.Sleep(100);
        }
    }
}