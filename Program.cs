using Disruptor;

namespace TestDisruptor;


public class Program
{
    public const int NumberRedisConsumer = 6;
    public const int NumberUpdateRedisConsumer = 6;
    public const int NumberPersistOracleByKafka = 4;
    public const int RingInSize = 16;
    public const int RingOutSize = 16;
    public const int NumberOfRequest = 10;
    static void Main()
    {
        var requests = GetTestRequests();
        var ringIn = BuildRingIn();
        long sequence = 0;
        RingInData ringInData = null;
        Dictionary<string, int> commandIndexDic = new Dictionary<string, int>();

        foreach (var req in requests)
        {
            if (!commandIndexDic.ContainsKey(req.ClientCode))
                commandIndexDic[req.ClientCode] = 1;
            else
                commandIndexDic[req.ClientCode] += 1;

            /// lấy sequence của ô sẵn sàng để ghi đè, trên ring in
            sequence = ringIn.Next();
            /// đọc data của ô đó
            ringInData = ringIn[sequence];
            /// ghi đè ClientCode của ô đó bằng ClientCode từ Request
            ringInData.ClientCode = req.ClientCode;
            /// gán thứ tự Request của Client bắt đầu từ 1
            ringInData.CommandIndex = commandIndexDic[req.ClientCode];
            /// gán LogConsumerId, đánh dấu LogConsumer sẽ xử lý request này
            ringInData.LogConsumerId = GetConsumerId(req.ClientCode, NumberUpdateRedisConsumer);
            /// gán RedisConsumerId, đánh dấu RedisConsumer sẽ xử lý request này
            ringInData.RedisConsumerId = GetConsumerId(req.ClientCode, NumberRedisConsumer);
            /// ghi đè RequestId của ô đó bằng RequestId từ Request
            ringInData.RequestId = req.Id;

            /// publish dữ liệu đã ghi đè vào ring
            ringIn.Publish(sequence);
        }
        Console.ReadKey();
    }

    public static int GetConsumerId(string clientCode, int numberConsumers)
    {
        var clientCodeHashed = clientCode.GetHashCode();
        var consumerId = Math.Abs(clientCodeHashed % numberConsumers) + 1;
        return consumerId;
    }
    ///Tạo list client codes từ 058C100000 -> 058C100003
    static List<string> GetClientCodes()
    {
        var list = new List<string>();
        for (int i = 100000; i < 100004; i++)
        {
            list.Add($"058C{i}");
        }
        return list;
    }

    /// Tạo list 100 Request của client codes từ 058C100000 -> 058C100003
    static List<Request> GetTestRequests()
    {
        var list = new List<Request>();
        var clientCodes = GetClientCodes();
        var cliendCodesCount = clientCodes.Count();
        var rand = new Random();
        for (int i = 0; i < NumberOfRequest; i++)
        {
            var req = new Request
            {
                ClientCode = clientCodes[rand.Next(0, cliendCodesCount)]
            };
            list.Add(req);
        }
        return list;
    }

    static RingBuffer<RingInData> BuildRingIn()
    {
        var disruptor = new Disruptor.Dsl.Disruptor<RingInData>(() => new RingInData(), RingInSize);
        disruptor.HandleEventsWith(BuildRedisConsumers(NumberRedisConsumer))
        .Then(BuildLogicConsumer());

        return disruptor.Start();

    }

    static UpdateRedisConsumer[] BuildUpdateRedisConsumers(int size)
    {
        var Consumers = new UpdateRedisConsumer[size];
        if (size > 0)
        {
            for (int idx = 1; idx <= size; idx++)
            {
                Consumers[idx - 1] = new UpdateRedisConsumer(idx);
            }
        }
        return Consumers;
    }

    static PersistOracleByKafkaConsumer[] BuildPersistOracleByKafkaConsumers(int size)
    {
        var Consumers = new PersistOracleByKafkaConsumer[size];
        if (size > 0)
        {
            for (int idx = 1; idx <= size; idx++)
            {
                Consumers[idx - 1] = new PersistOracleByKafkaConsumer(idx);
            }
        }
        return Consumers;
    }

    static RedisConsumer[] BuildRedisConsumers(int size)
    {
        var Consumers = new RedisConsumer[size];
        if (size > 0)
        {
            for (int idx = 1; idx <= size; idx++)
            {
                Consumers[idx - 1] = new RedisConsumer(idx);
            }
        }
        return Consumers;
    }

    static RingBuffer<RingOutData> BuildRingOut()
    {
        var disruptor = new Disruptor.Dsl.Disruptor<RingOutData>(() => new RingOutData(), RingOutSize);
        disruptor.HandleEventsWith(BuildUpdateRedisConsumers(NumberUpdateRedisConsumer))
        .Then(BuildPersistOracleByKafkaConsumers(NumberPersistOracleByKafka));

        return disruptor.Start();
    }

    static LogicConsumer BuildLogicConsumer()
    {
        return new LogicConsumer(BuildRingOut());
    }
}