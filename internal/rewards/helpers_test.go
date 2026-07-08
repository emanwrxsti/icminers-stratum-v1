package rewards

import "github.com/emanwrxsti/icminers-stratum-v1/internal/logging"

func testLogger() *logging.Logger {
	return logging.New(logging.Options{Level: "error"})
}
