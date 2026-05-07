using System.Threading.Tasks.Dataflow;

namespace AetherFlow.IngestionTests.PipelineTests;

[TestFixture]
public class PipelineTest
{
    [Test]
    public async Task Test1()
    {
        var block = new ActionBlock<string>(Console.WriteLine);

        foreach (var post in new[] { "hallo", "du", "da" })
        {
            block.Post(post);
        }

        block.Complete();
        await block.Completion;
        Console.WriteLine("servuvcs");
    }

    [Test]
    public async Task Test2()
    {
        var doubleIt = new TransformBlock<int, int>(i => i * 2);
        var output = new ActionBlock<int>(Console.WriteLine);

        // else every block need to be completed
        doubleIt.LinkTo(output, new DataflowLinkOptions { PropagateCompletion = true });


        foreach (var r in Enumerable.Range(1, 10))
        {
            // doubleIt.Post(r);
            await doubleIt.SendAsync(r);
        }
        // to say the pipe it is fin
        // nothing new
        doubleIt.Complete();
        // now Iam really fin
        await doubleIt.Completion;
    }
}