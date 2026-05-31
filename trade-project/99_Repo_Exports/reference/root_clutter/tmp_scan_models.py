import sys, glob, joblib, numpy as np
sys.path.append('.')
import core.ml_model_types

def _patch_modules():
    sys.modules['__main__'].UtilMHModelV1 = core.ml_model_types.UtilMHModelV1
    try:
        from core.fast_linear_util_mh import FastLinearUtilMHModel
        sys.modules['__main__'].FastLinearUtilMHModel = FastLinearUtilMHModel
    except:
        pass

_patch_modules()

paths = glob.glob('/var/lib/trade/ml_models/tb_v*/model.joblib')
paths.extend(glob.glob('/var/lib/trade/ml_models/edge_stack*/model.joblib'))
paths.extend(glob.glob('/var/lib/trade/ml_models/tb_stack*/model.joblib'))

for path in paths:
    try:
        model = joblib.load(path)
        if isinstance(model, core.ml_model_types.UtilMHModelV1):
            c_max = np.max(np.abs(model.ridge[60000].steps[-1][1].coef_))
            print(f"OK (UtilMHModelV1): {path} | max coef: {c_max}")
        elif isinstance(model, dict) and 'coefs' in model:
             if 60000 in model['coefs']:
                 c_max = np.max(np.abs(model['coefs'][60000]))
                 print(f"OK (dict): {path} | max coef: {c_max}")
             else:
                 print(f"OK (dict): {path} | no horizon 60000")
        elif hasattr(model, 'coefs'):
            c_max = np.max(np.abs(model.coefs[60000]))
            print(f"OK (FastLinear): {path} | max coef: {c_max}")
        else:
             print(f"Unknown type: {path} | {type(model)}")
    except Exception as e:
        print(f"ERR: {path} | {e}")
