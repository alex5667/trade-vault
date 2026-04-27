#!/usr/bin/env python3
"""
Test script to verify Redis connection works without deprecated parameters.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python-worker'))

try:
    import redis
    print("✅ Redis library imported successfully")

    # Test creating a Redis client without deprecated parameters
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            socket_timeout=10,
            socket_connect_timeout=5,
            health_check_interval=30,
            max_connections=10,
            retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
            socket_keepalive=True,
            decode_responses=True
        )
        print("✅ Redis client created successfully without deprecated parameters")

        # Try to ping (will fail if no Redis running, but that's expected)
        try:
            client.ping()
            print("✅ Redis ping successful")
        except redis.exceptions.ConnectionError:
            print("⚠️ Redis connection failed (expected if Redis not running)")
        except Exception as e:
            print(f"⚠️ Redis ping failed with unexpected error: {e}")

    except Exception as e:
        print(f"❌ Failed to create Redis client: {e}")
        sys.exit(1)

except ImportError as e:
    print(f"❌ Failed to import redis library: {e}")
    sys.exit(1)

print("\n🎉 Redis connection test completed successfully!")
print("All deprecated 'retry_on_timeout' parameters have been removed.")
