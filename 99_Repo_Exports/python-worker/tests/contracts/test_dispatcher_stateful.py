import pytest
import time
from services.signal_dispatcher import SignalDispatcher
from tests.contracts.test_dispatcher_stateful_model import run_state_machine

def test_stateful(dispatcher):  # используйте fixture dispatcher из предыдущих контракт-тестов
    run_state_machine(dispatcher)
