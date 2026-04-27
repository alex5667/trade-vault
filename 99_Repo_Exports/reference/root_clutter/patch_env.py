import re
text = open("docker-compose-python-workers.yml").read()
# Replace existing REDIS_URL and DATABASE_URL if any in these services
def replacer(match):
    block = match.group(0)
    # only within environment:
    if "environment:" in block:
        env_idx = block.find("environment:")
        if env_idx != -1:
            end_env_idx = block.find("    depends_on:", env_idx)
            if end_env_idx == -1: end_env_idx = block.find("    networks:", env_idx)
            if end_env_idx == -1: end_env_idx = len(block)
            
            env_block = block[env_idx:end_env_idx]
            
            # Remove any existing REDIS_URL or DATABASE_URL
            env_block_clean = re.sub(r' +REDIS_URL:.*\n', '', env_block)
            env_block_clean = re.sub(r' +DATABASE_URL:.*\n', '', env_block)
            
            # Add correct ones
            new_vars = "      REDIS_URL: ${REDIS_WORKER_URL:-redis://redis-worker-1:6379/0}\n      DATABASE_URL: ${POSTGRES_URL:-postgresql://trading:trading_password@postgres:5432/scanner_analytics}\n"
            
            new_env = env_block_clean.replace("environment:\n", "environment:\n" + new_vars)
            return block[:env_idx] + new_env + block[end_env_idx:]
    return block

new_text = re.sub(r'(  scanner-route-incident-rca-mirror-rca-winner-apply-apply.*?(?=\n  \w|\Z))', replacer, text, flags=re.DOTALL)
open("docker-compose-python-workers.yml", "w").write(new_text)
