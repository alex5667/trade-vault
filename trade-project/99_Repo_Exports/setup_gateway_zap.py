import re

filepath = "go-gateway/main.go"
with open(filepath, 'r') as f:
    content = f.read()

if "zap.ReplaceGlobals" not in content:
    init_code = """
	// Initialize structured logging
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)
"""
    # Insert after flag.Parse() or right inside main()
    if "func main() {" in content:
        content = content.replace("func main() {\n", "func main() {\n" + init_code)
    
    if '"go.uber.org/zap"' not in content:
        # We need to add imports
        content = content.replace('import (', 'import (\n\t"go.uber.org/zap"\n\t"go.uber.org/zap/zapcore"\n', 1)
    elif '"go.uber.org/zap/zapcore"' not in content:
        content = content.replace('"go.uber.org/zap"', '"go.uber.org/zap"\n\t"go.uber.org/zap/zapcore"')
        
    with open(filepath, 'w') as f:
        f.write(content)
