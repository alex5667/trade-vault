import sys

def main():
    lines = open("mega_patch_step6_metrics_alerts_signal_quality_v1.git.diff").readlines()
    out = []
    
    def replace_line(l):
        # We need to be careful to only replace paths for diff/---/+++ lines
        if l.startswith("diff --git ") or l.startswith("--- ") or l.startswith("+++ "):
            l = l.replace("a/services/orderflow/", "a/python-worker/services/orderflow/")
            l = l.replace("b/services/orderflow/", "b/python-worker/services/orderflow/")
            
            l = l.replace("a/tick_flow_full/", "a/reference/tick_flow_full/")
            l = l.replace("b/tick_flow_full/", "b/reference/tick_flow_full/")
            
            # Since orderflow_services is the root directory in the patch, let's prefix it
            l = l.replace("a/orderflow_services/", "a/python-worker/orderflow_services/")
            l = l.replace("b/orderflow_services/", "b/python-worker/orderflow_services/")
        return l
        
    for l in lines:
        out.append(replace_line(l))
        
    open("adjusted.diff", "w").writelines(out)

if __name__ == "__main__":
    main()
