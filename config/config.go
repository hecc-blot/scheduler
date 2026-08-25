package config

// Config 定时任务调度器配置。
type Config struct {
	// Location 时区，用于解析 cron 表达式。支持 IANA 名（如 "Asia/Shanghai"）、
	// "UTC" 或 "Local"。默认 Local（进程本地时区）。
	Location string `mapstructure:"location"`
	// AllowOverlap 同名任务上一次执行未结束、下一次触发时是否允许重叠执行。
	// 默认 false：跳过本次触发，避免对账/清理类任务并发重入。
	AllowOverlap bool `mapstructure:"allow_overlap"`
}

// Normalize 补全默认值，供构造调度器时调用。
func Normalize(cfg Config) Config {
	if cfg.Location == "" {
		cfg.Location = "Local"
	}
	return cfg
}
