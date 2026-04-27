import yaml
import os

def represent_dictionary_order(self, dict_data):
    return self.represent_mapping('tag:yaml.org,2002:map', dict_data.items())

def setup_yaml():
    yaml.add_representer(dict, represent_dictionary_order)

def fix_compose():
    setup_yaml()
    compose_file = 'docker-compose-python-workers.yml'
    
    with open(compose_file, 'r') as f:
        content = yaml.safe_load(f)
        
    for svc_name, svc_conf in content.get('services', {}).items():
        if 'environment' in svc_conf:
            env_vars = svc_conf['environment']
            if isinstance(env_vars, list):
                # Check if it has DB DSN but no timeout
                has_db = any('PG_DSN' in v or 'ANALYTICS_DB_DSN' in v or 'DATABASE_URL' in v for v in env_vars)
                has_timeout = any('PGCONNECT_TIMEOUT' in v for v in env_vars)
                
                if has_db and not has_timeout:
                    env_vars.append('PGCONNECT_TIMEOUT=${PGCONNECT_TIMEOUT:-15}')
                    env_vars.append('PGOPTIONS=${PGOPTIONS:-"-c statement_timeout=30000"}')
            elif isinstance(env_vars, dict):
                has_db = any(k in env_vars for k in ['PG_DSN', 'ANALYTICS_DB_DSN', 'DATABASE_URL'])
                has_timeout = 'PGCONNECT_TIMEOUT' in env_vars
                
                if has_db and not has_timeout:
                    env_vars['PGCONNECT_TIMEOUT'] = '${PGCONNECT_TIMEOUT:-15}'
                    env_vars['PGOPTIONS'] = '${PGOPTIONS:-"-c statement_timeout=30000"}'
                    
    # Write back
    temp_file = compose_file + '.patched'
    with open(temp_file, 'w') as f:
        yaml.dump(content, f, default_flow_style=False, sort_keys=False)
        
    os.rename(temp_file, compose_file)
    print("Injected Postgres connect timeouts to services in docker-compose-python-workers.yml.")

if __name__ == "__main__":
    fix_compose()
