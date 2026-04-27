#!/usr/bin/env python3
# Thin wrapper to keep imports stable. Full implementation lives in stream_consumer_impl.py
from stream_consumer_impl import StreamConsumer, main

__all__ = [
    'StreamConsumer',
    'main',
]

if __name__ == "__main__":
    main() 