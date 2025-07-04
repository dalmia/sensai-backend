import pytest
import asyncio
from unittest.mock import patch, AsyncMock
from src.api.utils.concurrency import async_batch_gather, async_index_wrapper


@pytest.mark.asyncio
class TestAsyncBatchGather:
    @patch("src.api.utils.concurrency.tqdm_asyncio")
    async def test_async_batch_gather_full_batch(self, mock_tqdm_asyncio):
        """Test async_batch_gather with a full batch."""
        # Create mock coroutines
        mock_coro1 = AsyncMock()
        mock_coro2 = AsyncMock()
        mock_coro3 = AsyncMock()

        # Setup the mock gather result
        mock_tqdm_asyncio.gather = AsyncMock()
        mock_tqdm_asyncio.gather.return_value = ["result1", "result2", "result3"]

        # Call the function with a batch size equal to the number of coroutines
        result = await async_batch_gather(
            [mock_coro1, mock_coro2, mock_coro3], batch_size=3, description="Test batch"
        )

        # Check the results
        assert result == ["result1", "result2", "result3"]
        mock_tqdm_asyncio.gather.assert_called_once_with(
            mock_coro1, mock_coro2, mock_coro3, desc="Test batch 0-3/3"
        )

    @patch("src.api.utils.concurrency.tqdm_asyncio")
    @patch("src.api.utils.concurrency.asyncio.sleep")
    async def test_async_batch_gather_multiple_batches(
        self, mock_sleep, mock_tqdm_asyncio
    ):
        """Test async_batch_gather with multiple batches."""
        # Create mock coroutines
        coroutines = [AsyncMock() for _ in range(5)]

        # Setup the mock gather results for each batch
        mock_tqdm_asyncio.gather = AsyncMock()
        # Make sure we provide enough return values for all calls
        # First call with batch size 2, second call with batch size 2, third call with batch size 1
        mock_tqdm_asyncio.gather.side_effect = [
            ["result1", "result2"],
            ["result3", "result4"],
            ["result5"],
        ]

        # Call the function with a batch size smaller than the number of coroutines
        result = await async_batch_gather(
            coroutines, batch_size=2, description="Test batch"
        )

        # Check the results
        assert result == ["result1", "result2", "result3", "result4", "result5"]

        # Check that gather was called with the correct batches
        assert mock_tqdm_asyncio.gather.call_count == 3
        mock_tqdm_asyncio.gather.assert_any_call(
            coroutines[0], coroutines[1], desc="Test batch 0-2/5"
        )
        mock_tqdm_asyncio.gather.assert_any_call(
            coroutines[2], coroutines[3], desc="Test batch 2-4/5"
        )
        mock_tqdm_asyncio.gather.assert_any_call(coroutines[4], desc="Test batch 4-5/5")

        # Check that sleep was called the correct number of times (once after each batch except the last)
        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(1)

    @patch("src.api.utils.concurrency.tqdm_asyncio")
    async def test_async_batch_gather_empty(self, mock_tqdm_asyncio):
        """Test async_batch_gather with an empty list of coroutines."""
        # Call the function with an empty list
        result = await async_batch_gather([], batch_size=2)

        # Check the results
        assert result == []
        mock_tqdm_asyncio.gather.assert_not_called()


@pytest.mark.asyncio
class TestAsyncIndexWrapper:
    async def test_async_index_wrapper(self):
        """Test async_index_wrapper."""

        # Create a mock async function
        async def mock_func(arg1, arg2):
            return f"{arg1}-{arg2}"

        # Call the wrapper function
        result = await async_index_wrapper(mock_func, 42, "test", "value")

        # Check the results
        assert result == (42, "test-value")
