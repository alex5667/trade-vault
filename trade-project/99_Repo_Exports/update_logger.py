import os

filepath = "go-worker/internal/logging/file_logger.go"
with open(filepath, 'r') as f:
    content = f.read()

new_setup = """// SetupGlobalFileLogger настраивает глобальный файловый логгер и Zap
func SetupGlobalFileLogger(logsDir string) error {
	var err error
	globalFileLoggerOnce.Do(func() {
		globalFileLogger, err = NewFileLogger(logsDir)
		if err == nil {
			// Настраиваем Zap logger
			config := zap.NewProductionEncoderConfig()
			config.EncodeTime = zapcore.ISO8601TimeEncoder
			jsonEncoder := zapcore.NewJSONEncoder(config)
			consoleEncoder := zapcore.NewJSONEncoder(config) // Консоль тоже JSON

			// Консольный вывод
			consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zap.DebugLevel)

			// Создаем WriteSyncer для FileLogger
			fileCore := &zapFileCore{
				logger:  globalFileLogger,
				encoder: consoleEncoder,
			}

			core := zapcore.NewTee(consoleCore, fileCore)
			logger := zap.New(core, zap.AddCaller())
			zap.ReplaceGlobals(logger)

			// Для обратной совместимости, если кто-то еще использует log.*
			zap.RedirectStdLog(logger)
		}
	})
	return err
}

type zapFileCore struct {
	logger  *FileLogger
	encoder zapcore.Encoder
}

func (c *zapFileCore) Enabled(lvl zapcore.Level) bool {
	return true
}

func (c *zapFileCore) With(fields []zapcore.Field) zapcore.Core {
	clone := c.encoder.Clone()
	for _, f := range fields {
		f.AddTo(clone)
	}
	return &zapFileCore{logger: c.logger, encoder: clone}
}

func (c *zapFileCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *zapFileCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	buf, err := c.encoder.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	
	levelStr := "INFO"
	if ent.Level >= zapcore.ErrorLevel {
		levelStr = "ERROR"
	} else if ent.Level == zapcore.WarnLevel {
		levelStr = "WARNING"
	}
	
	c.logger.Log(levelStr, buf.String().TrimSpace()) // wait, strings.TrimSpace doesn't exist on buf.String() directly like this in go. Need strings.TrimSpace(buf.String())
	return nil
}

func (c *zapFileCore) Sync() error {
	return nil
}

// GetGlobalFileLogger возвращает глобальный файловый логгер
func GetGlobalFileLogger() *FileLogger {
	return globalFileLogger
}
"""

# Fix the TrimSpace line
new_setup = new_setup.replace('buf.String().TrimSpace()', 'strings.TrimSpace(buf.String())')

try:
    start_idx = content.index("// SetupGlobalFileLogger")
    content = content[:start_idx] + new_setup
    
    if '"go.uber.org/zap"' not in content:
        content = content.replace("import (", "import (\n\t\"go.uber.org/zap\"\n\t\"go.uber.org/zap/zapcore\"\n", 1)

    with open(filepath, 'w') as f:
        f.write(content)
    print("Done")
except Exception as e:
    print(e)
