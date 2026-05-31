
import os

def fix_docker_compose():
    file_path = 'docker-compose-crypto-orderflow.yml'
    
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # 1. Extract the main environment list (from service 1)
    env_lines = []
    capture = False
    for line in lines:
        if 'environment: &id002' in line:
            capture = True
            continue
        if capture:
            # check indentation/format (usually "      - ...")
            if line.strip().startswith('- '):
                env_lines.append(line)
            else:
                # likely end of block
                if line.strip() and not line.strip().startswith('#'):
                     # if it unindents or matches another key
                     capture = False
    
    # Check what we captured
    print(f"Captured {len(env_lines)} env lines.")
    
    # 2. Modify SYMBOLS in the captured list for Service 2
    service2_env = []
    found_symbols = False
    for line in env_lines:
        if 'SYMBOLS=' in line:
            # indentation usually "      - "
            prefix = line.split('-')[0] + "- "
            new_line = prefix + "SYMBOLS=1000PEPEUSDT,DOGEUSDT,1000SHIBUSDT,1000FLOKIUSDT,1000BONKUSDT,WIFUSDT\n"
            service2_env.append(new_line)
            found_symbols = True
        else:
            service2_env.append(line)
            
    if not found_symbols:
        print("Warning: SYMBOLS not found in captured env!")

    # 3. Write new file, replacing the bad block in Service 2
    new_lines = []
    skip = False
    
    # We look for "crypto-orderflow-service-2:" -> "environment:"
    in_service_2 = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        if 'crypto-orderflow-service-2:' in line:
            in_service_2 = True
            new_lines.append(line)
            i += 1
            continue
            
        if in_service_2 and 'environment:' in line:
            # Write our new block
            prefix = line.split('environment:')[0]
            new_lines.append(f"{prefix}environment:\n")
            # Write all lines
            new_lines.extend(service2_env)
            
            # Skip the existing bad block (<<: *id002, SYMBOLS: ...)
            # We assume it is indented. We skip until we hit something less indented or "depends_on"
            i += 1
            while i < len(lines):
                curr = lines[i]
                if 'depends_on:' in curr:
                    # stop skipping
                    new_lines.append(curr)
                    i += 1
                    break
                # consume bad lines
                i += 1
            in_service_2 = False # done with this replacement
            continue
            
        new_lines.append(line)
        i += 1

    with open(file_path, 'w') as f:
        f.writelines(new_lines)
    print("Write complete.")

if __name__ == '__main__':
    fix_docker_compose()
