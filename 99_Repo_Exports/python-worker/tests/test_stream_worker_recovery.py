import unittest
from unittest.mock import MagicMock, call, patch
import redis
from news_pipeline.stream_worker import StreamWorker

class TestStreamWorkerRecovery(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock(spec=redis.Redis)
        # Mock _ensure_group to avoid calling it in __init__
        with patch('news_pipeline.stream_worker.StreamWorker._ensure_group'):
            self.worker = StreamWorker(
                redis=self.mock_redis,
                stream="test_stream",
                group="test_group",
                consumer="test_consumer",
                dlq_stream="test_dlq"
            )

    def test_run_forever_recovers_from_nogroup(self):
        # Mock _iter_batch to raise NOGROUP error once, then return empty
        # We need to control the loop execution.
        # run_forever calls _iter_batch.
        
        # Scenario:
        # 1. First call to _iter_batch fails with ResponseError("NOGROUP ...")
        #    This happens inside _iter_batch when calling xreadgroup usually, 
        #    but we need to verify where the exception is caught.
        #    Wait, _iter_batch calls xreadgroup. If xreadgroup raises, _iter_batch raises.
        #    So run_forever catches it.
        
        # We mock r.xreadgroup to raise the error
        self.worker.r.xreadgroup.side_effect = [
            redis.exceptions.ResponseError("NOGROUP No such key 'test_stream' or consumer group 'test_group'"),
            [], # Second call returns empty list
        ]
        
        # We also need to mock xautoclaim just in case, or make _iter_batch fail there?
        # _iter_batch calls xautoclaim first. Let's make that work or fail nicely.
        self.worker.r.xautoclaim.return_value = ("0-0", [])
        
        # We need to stop the loop after recovery.
        # We can set _shutdown to True after the first iteration?
        # But run_forever loop condition is `while not self._shutdown`.
        # Inside the loop:
        # try:
        #   _iter_batch() ...
        # except ResponseError:
        #   ...
        
        # We can use a side_effect on time.sleep to stop the loop?
        # distinct from the sleep inside the exception handler.
        
        # Better: Mock _ensure_group to set _shutdown=True so the loop exits after recovery attempt.
        # But _ensure_group is called inside the exception handler.
        # If we set _shutdown=True there, the loop check `while not self._shutdown` depends on where it is checked.
        # It's checked at the start of `while`.
        
        # Let's mock _ensure_group
        self.worker._ensure_group = MagicMock(side_effect=lambda: setattr(self.worker, '_shutdown', True))
        
        # And we need to ensure xgroup_create is called (inside our mock, or we verify the real _ensure_group logic separate)
        # Actually, since I patched methods on the instance, I'm testing the recovery flow.
        
        self.worker.run_forever()
        
        # Verify that warning was logged (we can't easily check log without capturing logs, but we can check if _ensure_group was called)
        self.worker._ensure_group.assert_called_once()


if __name__ == '__main__':
    unittest.main()
