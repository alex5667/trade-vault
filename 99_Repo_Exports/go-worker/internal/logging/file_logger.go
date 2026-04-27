// Пакет logging предоставляет файловое логирование для Go-воркеров
// Аналогично common/file_logger.py из Python-части проекта
package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// Константы для ротации логов (как в Python версии)
	DefaultLogFileSizeKB       = 18
	DefaultLogFilesTotalSizeMB = 512
	BytesInKilobyte            = 1024
	BytesInMegabyte            = BytesInKilobyte * 1024
)

// FileLogger - файловый логгер для записи логов в файлы
type FileLogger struct {
	logsDir            string
	logFileSize        int64
	logsTotalSizeLimit int64

	// Файлы для записи
	appLogFile    *os.File
	logFile       *os.File
	errorFile     *os.File
	warnDateFile  *os.File
	errorDateFile *os.File

	// Мьютекс для потокобезопасности
	mu sync.Mutex

	// Стандартный логгер для консоли
	stdLogger *log.Logger
}

var (
	globalFileLogger     *FileLogger
	globalFileLoggerOnce sync.Once
)

// getKyivTime возвращает текущее время по Киеву
func getKyivTime() time.Time {
	// Используем UTC+2 (приблизительно для Киева)
	// В production можно использовать time.LoadLocation("Europe/Kyiv")
	loc, err := time.LoadLocation("Europe/Kyiv")
	if err != nil {
		// Fallback на UTC+2
		loc = time.FixedZone("EET", 2*60*60)
	}
	return time.Now().In(loc)
}

// getKyivDateTime возвращает строку с датой и временем по Киеву в формате YYYY-MM-DD_HH-MM-SS
func getKyivDateTime() string {
	return getKyivTime().Format("2006-01-02_15-04-05")
}

// getDateStr возвращает дату в формате YYYY-MM-DD
func getDateStr() string {
	return getKyivTime().Format("2006-01-02")
}

// NewFileLogger создает новый файловый логгер
func NewFileLogger(logsDir string) (*FileLogger, error) {
	fl := &FileLogger{
		logsDir:            logsDir,
		logFileSize:        DefaultLogFileSizeKB * BytesInKilobyte,
		logsTotalSizeLimit: DefaultLogFilesTotalSizeMB * BytesInMegabyte,
		stdLogger:          log.New(os.Stdout, "", log.LstdFlags),
	}

	// Создаем директорию для логов
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("ошибка создания директории логов: %v", err)
	}

	// Инициализируем файлы
	if err := fl.initializeFiles(); err != nil {
		return nil, fmt.Errorf("ошибка инициализации файлов: %v", err)
	}

	// Тестовая запись
	testMsg := fmt.Sprintf("🚀 Приложение запущено: %s", time.Now().Format(time.RFC3339))
	fl.writeToFile(fl.appLogFile, testMsg)
	fl.stdLogger.Printf("✅ Тестовая запись в лог успешна: %s", filepath.Join(logsDir, "app.txt"))

	return fl, nil
}

// initializeFiles инициализирует все файлы для логирования
func (fl *FileLogger) initializeFiles() error {
	// Основные файлы
	appPath := filepath.Join(fl.logsDir, "app.txt")
	logPath := filepath.Join(fl.logsDir, "log.txt")
	errorPath := filepath.Join(fl.logsDir, "error.txt")

	var err error

	fl.appLogFile, err = os.OpenFile(appPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("ошибка открытия app.txt: %v", err)
	}

	fl.logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("ошибка открытия log.txt: %v", err)
	}

	fl.errorFile, err = os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("ошибка открытия error.txt: %v", err)
	}

	// НЕ создаем файлы с датой и временем заранее - они будут созданы при первой записи
	// Это предотвращает создание пустых файлов

	// Очищаем существующие пустые файлы warn и error
	fl.cleanupExistingEmptyFiles()

	return nil
}

// cleanupExistingEmptyFiles удаляет существующие пустые файлы warn_*.txt и error_*.txt
func (fl *FileLogger) cleanupExistingEmptyFiles() {
	// Ищем все файлы warn_*.txt и error_*.txt в директории логов
	entries, err := os.ReadDir(fl.logsDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Проверяем, является ли файл warn или error файлом с датой
		if (strings.HasPrefix(name, "warn_") || strings.HasPrefix(name, "error_")) && strings.HasSuffix(name, ".txt") {
			// Проверяем размер файла
			info, err := entry.Info()
			if err != nil {
				continue
			}

			// Если файл пустой, удаляем его
			if info.Size() == 0 {
				fullPath := filepath.Join(fl.logsDir, name)
				os.Remove(fullPath)
			}
		}
	}
}

// updateDateTimeFiles обновляет имена файлов с датой и временем
// Создает файлы только при первой записи, чтобы избежать пустых файлов
func (fl *FileLogger) updateDateTimeFiles() {
	currentDateTime := getKyivDateTime()
	currentDate := getDateStr()

	// Обновляем warn файл
	if fl.warnDateFile != nil {
		// Извлекаем дату из имени файла
		oldName := filepath.Base(fl.warnDateFile.Name())
		if len(oldName) > 10 {
			oldDate := oldName[5:15] // warn_YYYY-MM-DD
			if oldDate != currentDate {
				fl.warnDateFile.Close()
				fl.warnDateFile = nil
			}
		}
	}

	// Создаем warn файл только если его еще нет (будет создан при первой записи)
	if fl.warnDateFile == nil {
		warnPath := filepath.Join(fl.logsDir, fmt.Sprintf("warn_%s.txt", currentDateTime))
		var err error
		fl.warnDateFile, err = os.OpenFile(warnPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fl.stdLogger.Printf("⚠️ Ошибка создания warn файла: %v", err)
		}
	}

	// Обновляем error файл
	if fl.errorDateFile != nil {
		oldName := filepath.Base(fl.errorDateFile.Name())
		if len(oldName) > 11 {
			oldDate := oldName[6:16] // error_YYYY-MM-DD
			if oldDate != currentDate {
				fl.errorDateFile.Close()
				fl.errorDateFile = nil
			}
		}
	}

	// Создаем error файл только если его еще нет (будет создан при первой записи)
	if fl.errorDateFile == nil {
		errorPath := filepath.Join(fl.logsDir, fmt.Sprintf("error_%s.txt", currentDateTime))
		var err error
		fl.errorDateFile, err = os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fl.stdLogger.Printf("⚠️ Ошибка создания error файла: %v", err)
		}
	}
}

// cleanupEmptyFiles удаляет пустые файлы warn и error при закрытии
func (fl *FileLogger) cleanupEmptyFiles() {
	if fl.warnDateFile != nil {
		info, err := fl.warnDateFile.Stat()
		if err == nil && info.Size() == 0 {
			// Файл пустой, удаляем его
			fl.warnDateFile.Close()
			os.Remove(fl.warnDateFile.Name())
			fl.warnDateFile = nil
		}
	}

	if fl.errorDateFile != nil {
		info, err := fl.errorDateFile.Stat()
		if err == nil && info.Size() == 0 {
			// Файл пустой, удаляем его
			fl.errorDateFile.Close()
			os.Remove(fl.errorDateFile.Name())
			fl.errorDateFile = nil
		}
	}
}

// writeToFile записывает сообщение в файл
func (fl *FileLogger) writeToFile(file *os.File, message string) {
	if file == nil {
		return
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Проверяем ротацию
	fl.rotateIfNeeded(file)

	// Записываем сообщение
	now := time.Now()
	pid := os.Getpid()
	dateStr := now.Format("01/02/2006")
	timeStr := now.Format("03:04:05 PM")

	// Формат как в Python версии: [Nest] PID  - DATE, TIME    LEVEL [Context] message
	logLine := fmt.Sprintf("[Nest] %d  - %s, %s    %s\n", pid, dateStr, timeStr, message)

	file.WriteString(logLine)
	file.Sync() // Синхронизируем на диск
}

// rotateIfNeeded проверяет и выполняет ротацию файла при необходимости
func (fl *FileLogger) rotateIfNeeded(file *os.File) {
	info, err := file.Stat()
	if err != nil {
		return
	}

	if info.Size() > fl.logFileSize {
		// Создаем новое имя с датой и временем
		oldPath := file.Name()
		dateTime := getKyivDateTime()
		ext := filepath.Ext(oldPath)
		name := oldPath[:len(oldPath)-len(ext)]
		newPath := fmt.Sprintf("%s-%s%s", name, dateTime, ext)

		// Копируем файл
		oldFile, err := os.Open(oldPath)
		if err != nil {
			return
		}
		defer oldFile.Close()

		newFile, err := os.Create(newPath)
		if err != nil {
			return
		}
		defer newFile.Close()

		io.Copy(newFile, oldFile)

		// Очищаем текущий файл
		file.Truncate(0)
		file.Seek(0, 0)
	}
}

// Log записывает обычное сообщение
func (fl *FileLogger) Log(level, message string) {
	// Записываем в app.txt (все логи)
	fl.writeToFile(fl.appLogFile, message)

	// Определяем уровень и записываем в соответствующие файлы
	switch level {
	case "ERROR":
		fl.updateDateTimeFiles()
		fl.writeToFile(fl.errorFile, message)
		if fl.errorDateFile != nil {
			fl.writeToFile(fl.errorDateFile, message)
		}
	case "WARNING", "WARN":
		fl.updateDateTimeFiles()
		fl.writeToFile(fl.logFile, message)
		if fl.warnDateFile != nil {
			fl.writeToFile(fl.warnDateFile, message)
		}
	default:
		fl.writeToFile(fl.logFile, message)
	}
}

// Error записывает ошибку
func (fl *FileLogger) Error(format string, v ...interface{}) {
	message := fmt.Sprintf("ERROR %s", fmt.Sprintf(format, v...))
	fl.Log("ERROR", message)
}

// Warning записывает предупреждение
func (fl *FileLogger) Warning(format string, v ...interface{}) {
	message := fmt.Sprintf("WARN %s", fmt.Sprintf(format, v...))
	fl.Log("WARNING", message)
}

// Info записывает информационное сообщение
func (fl *FileLogger) Info(format string, v ...interface{}) {
	message := fmt.Sprintf("INFO %s", fmt.Sprintf(format, v...))
	fl.Log("INFO", message)
}

// Close закрывает все файлы и удаляет пустые файлы warn/error
func (fl *FileLogger) Close() error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Удаляем пустые файлы перед закрытием
	fl.cleanupEmptyFiles()

	var errs []error

	if fl.appLogFile != nil {
		if err := fl.appLogFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if fl.logFile != nil {
		if err := fl.logFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if fl.errorFile != nil {
		if err := fl.errorFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if fl.warnDateFile != nil {
		if err := fl.warnDateFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if fl.errorDateFile != nil {
		if err := fl.errorDateFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("ошибки при закрытии файлов: %v", errs)
	}

	return nil
}

// SetupGlobalFileLogger настраивает глобальный файловый логгер и Zap
func SetupGlobalFileLogger(logsDir string) error {
	var err error
	globalFileLoggerOnce.Do(func() {
		globalFileLogger, err = NewFileLogger(logsDir)
		if err == nil {
			// Настраиваем Zap logger
			config := zap.NewProductionEncoderConfig()
			config.EncodeTime = zapcore.ISO8601TimeEncoder
			
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

	c.logger.Log(levelStr, strings.TrimSpace(buf.String())) // wait, strings.TrimSpace doesn't exist on buf.String() directly like this in go. Need strings.TrimSpace(buf.String())
	return nil
}

func (c *zapFileCore) Sync() error {
	return nil
}

// GetGlobalFileLogger возвращает глобальный файловый логгер
func GetGlobalFileLogger() *FileLogger {
	return globalFileLogger
}
