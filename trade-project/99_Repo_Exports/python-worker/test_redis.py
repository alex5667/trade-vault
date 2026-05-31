import asyncio
import redis.asyncio as redis
async def main():
    r = redis.from_url('redis://go_gateway:fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130@redis-worker-1:6379/0')
    print("ping: ", await r.ping())
    try:
        await r.xgroup_create("stream:ml:operator_rca_routing_incident_bundle_requests", "operator_rca_routing_incident_bundle_builder_v2_8", id="$", mkstream=True)
        print("Group created")
    except Exception as e:
        print("Group create err:", e)
    rows = await r.xreadgroup("operator_rca_routing_incident_bundle_builder_v2_8", "test", {"stream:ml:operator_rca_routing_incident_bundle_requests": ">"}, count=1, block=5000)
    print("Rows: ", rows)
asyncio.run(main())
