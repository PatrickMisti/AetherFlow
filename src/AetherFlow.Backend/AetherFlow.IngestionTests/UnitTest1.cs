using System.Threading.Tasks.Dataflow;

namespace AetherFlow.IngestionTests;

public class Tests
{
    [Test]
    public async Task Test1()
    {
        // 1. Blöcke erstellen
        var buffer = new BufferBlock<int>();

        var transform = new TransformBlock<int, string>(
            x => $"Verarbeitet: {x * x}",
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4 }
        );

        var action = new ActionBlock<string>(
            s => Console.WriteLine(s)
        );

        // 2. Blöcke verbinden (LinkTo)
        buffer.LinkTo(transform, new DataflowLinkOptions { PropagateCompletion = true });
        transform.LinkTo(action, new DataflowLinkOptions { PropagateCompletion = true });

        // 3. Daten hineinsenden
        for (int i = 1; i <= 10; i++)
        {
            buffer.Post(i);           // synchron, gibt false zurück wenn voll
            // oder:
            await buffer.SendAsync(i); // asynchron, wartet wenn voll
        }

        // 4. Pipeline beenden und auf Abschluss warten
        buffer.Complete();
        await action.Completion;

    }

    [Test]
    public async Task Test2()
    {
        var broadcast = new BroadcastBlock<int>(x => x);

        var logBlock = new ActionBlock<int>(
            x => Console.WriteLine(x),
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 2 }
        );

        var processBlock = new TransformBlock<int, string>(
            x => $"Verarbeitet: {x * x}",
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4 }
        );

        broadcast.LinkTo(logBlock, new DataflowLinkOptions { PropagateCompletion = true });
        broadcast.LinkTo(processBlock, new DataflowLinkOptions { PropagateCompletion = true });

        await broadcast.SendAsync(5);

        broadcast.Complete();
        await broadcast.Completion;
    }
}