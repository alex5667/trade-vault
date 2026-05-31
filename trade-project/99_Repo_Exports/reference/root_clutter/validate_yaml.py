import yaml
import sys

def check_duplicate_keys(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            print(f"DUPLICATE KEY FOUND: {key} at line {key_node.start_mark.line + 1}")
        mapping[key] = value_node
    return loader.construct_mapping(node, deep)

yaml.SafeLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, check_duplicate_keys)

def check_yaml(filename):
    try:
        with open(filename, "r") as f:
            yaml.safe_load(f)
        print(f"File {filename} parsed (duplicates check via constructor)")
    except yaml.YAMLError as exc:
        print(f"Error in {filename}:\n{exc}")

if __name__ == "__main__":
    check_yaml("docker-compose-timers.yml")
