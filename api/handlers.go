package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/kireetivar/async-job-queue/models"
)

// ErrorResponse represents an error response body.
type ErrorResponse struct {
	Error string `json:"error" example:"something went wrong"`
}

// StatusResponse represents a generic status response body.
type StatusResponse struct {
	Status string `json:"status" example:"success"`
}

type CreateJob struct {
	Queue      string          `json:"queue" binding:"required" example:"emails"`
	Type       string          `json:"type" binding:"required" example:"send_welcome_email"`
	Payload    json.RawMessage `json:"payload" swaggertype:"object"`
	Priority   int             `json:"priority" example:"5"`
	MaxRetries int             `json:"max_retries" example:"3"`
}

// createJob enqueues a new job.
// @Summary      Enqueue a new job
// @Description  Create and enqueue a new job to the specified queue with a payload and priority.
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Param        job  body      CreateJob  true  "Job creation payload"
// @Success      201  {object}  models.Job
// @Failure      400  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /jobs [post]
func (r *Router) createJob(c *gin.Context) {
	var req CreateJob

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jobId := uuid.New().String()

	job := &models.Job{
		ID:         jobId,
		Queue:      req.Queue,
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		Status:     models.StatusEnqueued,
		CreatedAt:  time.Now(),
	}

	if err := r.store.Enqueue(c, job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, job)

}

// getJob inspects a single job by its ID.
// @Summary      Get a job by ID
// @Description  Retrieve the full details and current status of a job by its UUID.
// @Tags         jobs
// @Produce      json
// @Param        id   path      string  true  "Job ID (UUID)"
// @Success      200  {object}  models.Job
// @Failure      404  {object}  ErrorResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /jobs/{id} [get]
func (r *Router) getJob(c *gin.Context) {
	jobId := c.Param("id")
	job, err := r.store.GetJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
		return
	}

	c.JSON(http.StatusOK, job)
}

// deleteJob cancels a pending job.
// @Summary      Cancel a job
// @Description  Cancel a pending or delayed job by its ID. Removes it from both active and delayed queues.
// @Tags         jobs
// @Produce      json
// @Param        id   path      string  true  "Job ID (UUID)"
// @Success      204  "No Content"
// @Failure      500  {object}  ErrorResponse
// @Router       /jobs/{id} [delete]
func (r *Router) deleteJob(c *gin.Context) {
	jobId := c.Param("id")
	err := r.store.CancelJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

// getQueues lists all queues and their depth.
// @Summary      List all queues
// @Description  Returns a list of all known queues with their current depth and pause status.
// @Tags         queues
// @Produce      json
// @Success      200  {array}   models.QueueInfo
// @Failure      500  {object}  ErrorResponse
// @Router       /queues [get]
func (r *Router) getQueues(c *gin.Context) {
	queues, err := r.store.ListQueues(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, queues)
}

// resumeQueue resumes a paused queue.
// @Summary      Resume a queue
// @Description  Resume processing for a previously paused queue. Workers will begin dequeuing jobs again.
// @Tags         queues
// @Produce      json
// @Param        name path      string  true  "Queue name"
// @Success      200  {object}  StatusResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /queues/{name}/resume [post]
func (r *Router) resumeQueue(c *gin.Context) {
	name := c.Param("name")
	err := r.store.ResumeQueue(c, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// pauseQueue pauses a queue.
// @Summary      Pause a queue
// @Description  Pause a queue so workers stop dequeuing new jobs from it. In-flight jobs will complete.
// @Tags         queues
// @Produce      json
// @Param        name path      string  true  "Queue name"
// @Success      200  {object}  StatusResponse
// @Failure      500  {object}  ErrorResponse
// @Router       /queues/{name}/pause [post]
func (r *Router) pauseQueue(c *gin.Context) {
	name := c.Param("name")
	err := r.store.PauseQueue(c, name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// getHealth returns dashboard queue health stats.
// @Summary      Queue health dashboard
// @Description  Returns health statistics for all queues including pending, dead counts and pause status.
// @Tags         monitoring
// @Produce      json
// @Success      200  {array}   models.QueueStats
// @Failure      500  {object}  ErrorResponse
// @Router       /dashboard [get]
func (r *Router) getHealth(c *gin.Context) {
	queueStatus, err := r.store.GetQueueStatus(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, queueStatus)
}

// retryJob manually retries a dead or failed job.
// @Summary      Retry a job
// @Description  Reset a dead or failed job and re-enqueue it for processing. Only works on jobs with status dead or failed.
// @Tags         jobs
// @Produce      json
// @Param        id   path      string  true  "Job ID (UUID)"
// @Success      200  {object}  StatusResponse
// @Failure      404  {object}  ErrorResponse
// @Failure      406  {object}  ErrorResponse  "Job is not in a retryable state"
// @Failure      500  {object}  ErrorResponse
// @Router       /jobs/{id}/retry [post]
func (r *Router) retryJob(c *gin.Context) {
	jobId := c.Param("id")
	job, err := r.store.GetJob(c, jobId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	status := job.Status
	if status != models.StatusDead && status != models.StatusFailed {
		c.JSON(http.StatusNotAcceptable, gin.H{"error": "can only retry dead or failed jobs"})
		return
	}
	job.RetryCount = 0
	job.Status = models.StatusEnqueued
	err = r.store.Enqueue(c, job)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "retry successful"})
}

// healthCheck performs a basic liveness check.
// @Summary      Health check
// @Description  Returns OK if the service is alive and can reach Redis.
// @Tags         monitoring
// @Produce      json
// @Success      200  {object}  StatusResponse
// @Failure      503  {object}  ErrorResponse
// @Router       /health [get]
func (r *Router) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
