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
        // signal that no more input will arrive
        doubleIt.Complete();
        // doubleIt finishing isn't the end of the pipeline — completion propagates to `output`,
        // so await output.Completion to know everything was processed
        await output.Completion;
    }
}