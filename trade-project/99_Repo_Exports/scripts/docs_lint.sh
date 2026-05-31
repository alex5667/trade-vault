#!/bin/bash
set -e

echo "🔍 Running Documentation Lint (Link Checker)..."

BASE_DIR="documentation/full_guide"
FAILED=0

# Check for broken internal links
# Find all .md files in the base directory
files=$(find "$BASE_DIR" -name "*.md")

for f in $files; do
    echo "Checking $f..."
    
    # Extract links like [text](target.md)
    # We use a more robust regex to find markdown links and extract the URL part
    grep -oP '\[.*?\]\(\K.*?(?=\))' "$f" | while read -r link; do
        # Ignore external links, mailto, and anchors only
        if [[ "$link" =~ ^http ]] || [[ "$link" =~ ^mailto ]] || [[ "$link" == \#* ]]; then
            continue
        fi

        # Remove anchor if present
        target=$(echo "$link" | cut -d'#' -f1)
        
        # If target becomes empty (was just an anchor), skip
        if [ -z "$target" ]; then
            continue
        fi

        # Determine full path
        if [[ "$target" == /* ]]; then
            # Absolute from repo root
            fullpath=".$target"
        else
            # Relative to current file's directory
            dir=$(dirname "$f")
            fullpath="$dir/$target"
        fi
        
        # Check existence
        if [ ! -f "$fullpath" ]; then
            # Try once more with repo root if relative fails and looks like a repo-relative path
            if [ ! -f "$target" ]; then
                 echo "  ❌ Broken link in $f: $link (Target $fullpath not found)"
                 FAILED=1
            fi
        fi
    done
done

if [ $FAILED -eq 1 ]; then
    echo "❌ Documentation lint FAILED."
    exit 1
else
    echo "✅ Documentation lint PASSED."
    exit 0
fi
