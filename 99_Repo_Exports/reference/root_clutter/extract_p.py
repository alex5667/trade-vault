import os

patch_file = 'ml_phase3_30_route_incident_rca_mirror_rca_winner_apply_apply_experiment_v1.patch'
files_extracted = 0

try:
    with open(patch_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    out_fh = None
    
    for line in lines:
        if line.startswith('+++ b/'):
            if out_fh:
                out_fh.close()
            filename = line.strip().split('+++ b/')[1]
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            out_fh = open(filename, 'w', encoding='utf-8')
            files_extracted += 1
        elif line.startswith('+') and not line.startswith('+++'):
            if out_fh:
                out_fh.write(line[1:])
        elif line.startswith(' ') and out_fh:
            out_fh.write(line[1:])
        elif line.startswith('diff --git'):
            if out_fh:
                out_fh.close()
                out_fh = None

    if out_fh:
        out_fh.close()
    print(f'Extracted {files_extracted} files.')
    
except Exception as e:
    print(f'Error: {e}')
