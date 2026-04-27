awk '
/^networks:$/ {
    print "  scanner-ml-rca-telegram-summary:"
    print "    build:"
    print "      context: ."
    print "      dockerfile: python-worker/Dockerfile"
    print "    container_name: scanner-ml-rca-telegram-summary"
    print "    environment:"
    print "      - REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0}"
    print "      - ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_DECISIONS_STREAM=stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions"
    print "      - ML_RCA_TG_SUMMARY_INTERVAL_SEC=3600"
    print "    depends_on:"
    print "      redis-worker-1:"
    print "        condition: service_healthy"
    print "    networks:"
    print "      - scanner-core"
    print "      - scanner-infra"
    print "    restart: on-failure:5"
    print "    deploy:"
    print "      resources:"
    print "        limits:"
    print "          memory: 256M"
    print "          cpus: \0470.2\047"
    print "    command: python -u services/ml_rca_telegram_summary_service_v1.py"
    print ""
    print $0
    next
}
{print}
' docker-compose-python-workers.yml > docker-compose-python-workers.yml.tmp && mv docker-compose-python-workers.yml.tmp docker-compose-python-workers.yml
