import glob

files = glob.glob('python-worker/**/ml_feature_schema_v1[0-9]*.py', recursive=True)

patch = """
import warnings
import logging
logger = logging.getLogger(__name__)
msg = f"This feature schema version is DEPRECATED (causes data leakage). See DEPRECATED_SCHEMAS in feature_registry."
warnings.warn(msg, DeprecationWarning, stacklevel=2)
logger.error(msg)
"""

for f in files:
    with open(f, 'r') as fp:
        content = fp.read()
    
    if patch in content:
        content = content.replace(patch, "")
        with open(f, 'w') as fp:
            fp.write(content)
        print(f"Reverted {f}")
