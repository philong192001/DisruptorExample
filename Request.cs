namespace TestDisruptor;

public class Request
{
    public string ClientCode { get; set; }
    public string Id { get; set; } = Guid.NewGuid().ToString().Substring(0, 8);
}