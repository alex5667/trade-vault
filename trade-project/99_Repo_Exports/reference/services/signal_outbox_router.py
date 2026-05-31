import sys
import os

# Ensure we can import from the root if needed
sys.path.append(os.getcwd())

from services.signal_outbox_dispatcher import SignalDispatcher

if __name__ == "__main__":
    SignalDispatcher().run()
