package config

import (
	"fmt"
	"os"
)

type Config struct {
	RedisConfig
	DatabaseConfig
}

type RedisConfig struct {
	Host string
	Port string
}

type DatabaseConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Name     string
}

func (c *DatabaseConfig) ConnString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", c.User, c.Password, c.Host, c.Port, c.Name)
}

func ReadFromEnv() *Config {
	// TODO: idk how I feel about this. I'd rather have the envars defined in the entrypoint
	// 	This does make sense if there were multiple binaries that share config values.
	c := &Config{
		RedisConfig: RedisConfig{
			Host: os.Getenv("REDIS_HOST"),
			Port: os.Getenv("REDIS_PORT"),
		},
		DatabaseConfig: DatabaseConfig{
			User:     os.Getenv("DB_USER"),
			Password: os.Getenv("DB_PASS"),
			Host:     os.Getenv("DB_HOST"),
			Port:     os.Getenv("DB_PORT"),
			Name:     os.Getenv("DB_NAME"),
		},
	}
	return c
}
