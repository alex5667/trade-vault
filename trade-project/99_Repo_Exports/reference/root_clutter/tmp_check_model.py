import sys, json, numpy as np

try:
    sys.path.append('.')
    import joblib
    import core.ml_model_types
    sys.modules['__main__'].UtilMHModelV1 = core.ml_model_types.UtilMHModelV1
    
    path = '/var/lib/trade/ml_models/edge_stack_v1/runs/20260311_000037/edge_stack_v1.joblib'
    model = joblib.load(path)
    print("Type:", type(model))
    if isinstance(model, dict):
        print("Kind:", model.get('kind'))
        if 'coefs' in model:
             print("Coef sum:", sum([np.sum(np.abs(v)) for v in model['coefs'].values()]))
        if 'meta_lr' in model:
             print("Meta LR max coef:", np.max(np.abs(model['meta_lr'].coef_)))
    
    for h in model.horizons:
        print(f"\n--- Horizon {h} ---")
        ridge_m = model.ridge[h]
        gbdt_m = model.gbdt[h]
        
        print("Ridge pipeline steps:", getattr(ridge_m, 'steps', 'None'))
        if hasattr(ridge_m, 'steps'):
            for name, step in ridge_m.steps:
                print(f"  Step {name}: {type(step)}")
                if hasattr(step, 'coef_'):
                    print(f"    Coef max: {np.max(np.abs(step.coef_))} sum: {np.sum(np.abs(step.coef_))}")
                if hasattr(step, 'constant'):
                    print(f"    Constant: {step.constant}")
                
        print("GBDT type:", type(gbdt_m))
        print("GBDT is_dummy:", str(type(gbdt_m)).find('Dummy') != -1)
        if hasattr(gbdt_m, 'constant'):
            print("GBDT constant:", gbdt_m.constant)
        
except Exception as e:
    import traceback
    traceback.print_exc()
