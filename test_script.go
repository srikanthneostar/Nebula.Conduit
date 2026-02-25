package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Xecutables/Nebula.Conduit/config" // or another valid implementation
	"github.com/Xecutables/Nebula.Conduit/internal/task"
	"github.com/Xecutables/Nebula.Conduit/pkg/executor"

	_ "github.com/mattn/go-sqlite3"
)

func testExecutor() {

	basepath := os.Getenv("NEBULA_CONDUIT_HOME")
	if basepath == "" {
		log.Fatal("Environment variable NEBULA_CONDUIT_HOME is not set.")
	}

	// sqlPath := filepath.Join(basepath, "migrations", "Nebula.Conduit.db")
	sqlPath := "C:/Nebula.Conduit/commons/Nebula.Conduit.db"
	fmt.Printf("Using database path: %s\n", sqlPath)
	db, err := sql.Open("sqlite3", sqlPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	taskRepo := task.NewSQLiteRepository(db)
	pathCfg := &config.PathConfig{
		PythonScriptsHome: "C:/nebula.rivulet/fabric/build/lib/algorithms",
		AllowedPaths:      []string{"C:/nebula.rivulet/fabric/build/lib/algorithms"},
	}
	timeout := 10 * time.Second

	// Create an instance of executor
	pythonExecutor := executor.NewPythonExecutor(taskRepo, pathCfg, timeout)

	// Prepare input
	// taskID := "test-task-id"
	scriptName := "pattern_identification.py"
	args := []string{"--queryid1", "5", "--queryid2", "6"}
	env := []string{}
	userID := 1

	// Call RunTask
	ctx := context.Background()
	task, _ := pythonExecutor.ExecuteWithTimeout(ctx, scriptName, args, env, userID)

	if task == nil {
		log.Fatal("Task execution failed, task is nil")
	}

	pythonExecutor.RunTask(ctx, task.ID, scriptName, args, env, userID)

	// Fetch and print task info
	// task, err := taskRepo.GetTask(taskID)
	// if err != nil {
	// 	log.Fatalf("Failed to get task: %v", err)
	// }

	// log.Printf("Task finished: %+v\n", task)
}

func main() {
	testExecutor()
}
