using System.Threading.Tasks.Dataflow;
using AetherFlow.Domain.Domains;

namespace AetherFlow.Shared.Pipeline;

public static class AetherPipelineUtils
{
    extension(ISourceBlock<AetherChunk?> block)
    {
        public TransformBlock<AetherChunk, AetherChunk?> Link(Func<AetherChunk, AetherChunk?> func,
            Predicate<AetherChunk?> predicate, DataflowLinkOptions linkOpts, ExecutionDataflowBlockOptions blockOpts)
        {
            var transformBlock =
                new TransformBlock<AetherChunk, AetherChunk?>(transform: func, dataflowBlockOptions: blockOpts);
            
            block.LinkTo(
                target: transformBlock!, 
                linkOptions: linkOpts, 
                predicate: predicate);
            
            return transformBlock;
        }
    }
}