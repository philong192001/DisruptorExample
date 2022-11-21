using Disruptor;

namespace TestDisruptor;


public class LogicConsumer : IEventHandler<RingInData>
{
    private readonly RingBuffer<RingOutData> _ringOut;

    public LogicConsumer(RingBuffer<RingOutData> ringOut)
    {
        _ringOut = ringOut;
    }
    public void OnEvent(RingInData data, long sequence, bool endOfBatch)
    {
        Console.WriteLine($"---- (C2) {data.ClientCode} {data.RequestId} - LOGIC handler for command {data.CommandIndex} ");
        /// Lấy sequence của ô sẵn sàng để ghi đè trong ring out
        var seq = _ringOut.Next();
        /// Đọc data từ ô đó ra
        var ringOutData = _ringOut[seq];
        /// Ghi đè ClientCode bằng ClientCode từ ring in
        ringOutData.ClientCode = data.ClientCode;
        ringOutData.CommandIndex = data.CommandIndex;
        ringOutData.PersistOracleByKafkaConsumerId = Program.GetConsumerId(data.ClientCode, Program.NumberPersistOracleByKafka);
        ringOutData.UpdateRedisConsumerId = Program.GetConsumerId(data.ClientCode, Program.NumberUpdateRedisConsumer);
        ringOutData.RequestId = data.RequestId;

        /// publish dữ liệu đã ghi đè vào ring out
        _ringOut.Publish(seq);

        Thread.Sleep(50);
    }
}