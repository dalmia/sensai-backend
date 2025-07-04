from typing import List, Coroutine
import asyncio
from tqdm.asyncio import tqdm_asyncio


async def async_batch_gather(
    coroutines: List[Coroutine],
    batch_size: int = 25,
    description: str = "Processing batch",
):
    total_num = len(coroutines)
    # outputs = [None] * total_num
    results = []

    # Process in batches to limit memory usage
    for i in range(0, total_num, batch_size):
        batch = coroutines[i : i + batch_size]
        batch_results = await tqdm_asyncio.gather(
            *batch,
            desc=f"{description} {i}-{i+len(batch)}/{total_num}",
        )
        results.extend(batch_results)

        # for completed_task in asyncio.as_completed(batch):
        #     task_row_index, output = await completed_task

        #     outputs[task_row_index] = output
        #     pbar.update(1)

        # Give a little time for memory to be freed up
        await asyncio.sleep(1)

    return results


async def async_index_wrapper(func, index, *args, **kwargs):
    output = await func(*args, **kwargs)
    return index, output
