# FR-002: Pause/Resume Job Functionality

**Status**: Open  
**Priority**: Medium  
**Complexity**: High  
**Estimated Effort**: 2-3 weeks  

## 📋 **Summary**

Implement proper pause/resume functionality for MultiJobSqlServer jobs that actually suspends and resumes job execution, unlike the previously removed broken implementation.

## 🎯 **Problem Statement**

### **Current Limitation**
- Jobs can only be **stopped** (terminated) or **running**
- No way to temporarily suspend a job while preserving state
- Stopping a job loses all runtime context and requires full redeployment
- Operators need pause/resume for maintenance windows, debugging, and resource management

### **Use Cases**
1. **Maintenance Windows**: Pause jobs during system maintenance without losing state
2. **Resource Management**: Temporarily free resources for high-priority tasks
3. **Debugging**: Pause suspect jobs for investigation while preserving context
4. **Gradual Rollouts**: Pause old jobs before starting new versions
5. **Cost Optimization**: Pause non-critical jobs during peak pricing periods

## 🚀 **Proposed Solution**

### **New API Methods**

```rust
impl MultiJobSqlServer {
    /// Pause a running job (suspends message processing)
    pub async fn pause_job(&self, name: &str) -> Result<(), SqlError>;
    
    /// Resume a paused job (restarts message processing)
    pub async fn resume_job(&self, name: &str) -> Result<(), SqlError>;
    
    /// Get detailed job state including pause information
    pub async fn get_job_details(&self, name: &str) -> Option<JobDetails>;
}
```

### **Enhanced Job Status**

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum JobStatus {
    Starting,
    Running,
    Paused {
        paused_at: chrono::DateTime<chrono::Utc>,
        reason: Option<String>,
    },
    Stopped,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct JobDetails {
    pub name: String,
    pub status: JobStatus,
    pub metrics: JobMetrics,
    pub pause_history: Vec<PauseEvent>,
    pub kafka_lag: Option<i64>,
    pub last_processed_offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PauseEvent {
    pub paused_at: chrono::DateTime<chrono::Utc>,
    pub resumed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub duration: Option<std::time::Duration>,
    pub reason: Option<String>,
}
```

## 🏗️ **Technical Design**

### **1. Job Control Architecture**

```rust
#[derive(Debug)]
pub enum JobControl {
    Pause { reason: Option<String> },
    Resume,
    Stop,
    UpdateMetrics,
    GetStatus,
}

#[derive(Debug)]
pub enum JobStatusUpdate {
    Started,
    Paused { kafka_lag: i64, last_offset: i64 },
    Resumed { kafka_lag: i64 },
    RecordsProcessed(u64),
    Error(String),
    Stopped,
}
```

### **2. Enhanced Job Execution Loop**

```rust
async fn job_execution_loop(
    job_name: String,
    sql_query: String,
    kafka_config: KafkaConfig,
    mut control_receiver: mpsc::UnboundedReceiver<JobControl>,
    status_sender: mpsc::UnboundedSender<JobStatusUpdate>,
) -> Result<(), SqlError> {
    let mut execution_state = ExecutionState::Running;
    let mut kafka_consumer = create_kafka_consumer(&kafka_config).await?;
    let mut pause_start: Option<Instant> = None;
    
    loop {
        tokio::select! {
            // Handle control commands
            control_msg = control_receiver.recv() => {
                match control_msg {
                    Some(JobControl::Pause { reason }) => {
                        if matches!(execution_state, ExecutionState::Running) {
                            execution_state = ExecutionState::Paused;
                            pause_start = Some(Instant::now());
                            
                            // Commit current offsets before pausing
                            kafka_consumer.commit().await?;
                            
                            // Get current lag for reporting
                            let lag = kafka_consumer.get_lag().await?;
                            let last_offset = kafka_consumer.get_last_offset().await?;
                            
                            status_sender.send(JobStatusUpdate::Paused {
                                kafka_lag: lag,
                                last_offset,
                            }).ok();
                            
                            info!("Job '{}' paused. Reason: {:?}", job_name, reason);
                        }
                    }
                    
                    Some(JobControl::Resume) => {
                        if matches!(execution_state, ExecutionState::Paused) {
                            execution_state = ExecutionState::Running;
                            
                            if let Some(start) = pause_start.take() {
                                let pause_duration = start.elapsed();
                                info!("Job '{}' resumed after {:?}", job_name, pause_duration);
                            }
                            
                            let lag = kafka_consumer.get_lag().await?;
                            status_sender.send(JobStatusUpdate::Resumed { kafka_lag: lag }).ok();
                        }
                    }
                    
                    Some(JobControl::Stop) => {
                        info!("Job '{}' stopping", job_name);
                        kafka_consumer.close().await;
                        status_sender.send(JobStatusUpdate::Stopped).ok();
                        break;
                    }
                    
                    None => break, // Channel closed
                }
            }
            
            // Process Kafka messages (only when running)
            kafka_msg = kafka_consumer.poll(Duration::from_millis(100)), 
                if matches!(execution_state, ExecutionState::Running) => {
                match kafka_msg {
                    Ok(Some(message)) => {
                        // Process SQL query on message
                        process_sql_message(&sql_query, message).await?;
                        
                        status_sender.send(JobStatusUpdate::RecordsProcessed(1)).ok();
                    }
                    Ok(None) => {
                        // No message available, continue polling
                    }
                    Err(e) => {
                        status_sender.send(JobStatusUpdate::Error(e.to_string())).ok();
                        // Continue processing unless critical error
                    }
                }
            }
            
            // When paused, periodic status updates
            _ = tokio::time::sleep(Duration::from_secs(10)), 
                if matches!(execution_state, ExecutionState::Paused) => {
                let lag = kafka_consumer.get_lag().await.unwrap_or(-1);
                // Send periodic status updates while paused
                status_sender.send(JobStatusUpdate::Paused {
                    kafka_lag: lag,
                    last_offset: kafka_consumer.get_last_offset().await.unwrap_or(-1),
                }).ok();
            }
        }
    }
    
    Ok(())
}

#[derive(Debug)]
enum ExecutionState {
    Running,
    Paused,
}
```

### **3. Enhanced RunningJob Structure**

```rust
#[derive(Debug)]
pub struct RunningJob {
    pub name: String,
    pub version: String,
    pub query: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    
    // Enhanced control
    pub control_sender: mpsc::UnboundedSender<JobControl>,
    pub status_receiver: Arc<Mutex<mpsc::UnboundedReceiver<JobStatusUpdate>>>,
    pub execution_handle: JoinHandle<Result<(), SqlError>>,
    
    // Enhanced state
    pub metrics: JobMetrics,
    pub pause_history: Vec<PauseEvent>,
    pub kafka_state: Option<KafkaJobState>,
}

#[derive(Debug, Clone)]
pub struct KafkaJobState {
    pub consumer_group_id: String,
    pub current_lag: i64,
    pub last_processed_offset: i64,
    pub assigned_partitions: Vec<i32>,
}
```

## 📝 **Implementation Requirements**

### **Phase 1: Core Pause/Resume (Week 1)**
- [ ] Implement `JobControl` enum and communication channels
- [ ] Add `pause_job()` and `resume_job()` methods to `MultiJobSqlServer`
- [ ] Enhance job execution loop with pause/resume logic
- [ ] Add `JobStatus::Paused` variant with metadata
- [ ] Basic unit tests for pause/resume functionality

### **Phase 2: State Management (Week 2)**
- [ ] Implement Kafka offset management during pause/resume
- [ ] Add pause history tracking
- [ ] Implement lag monitoring while paused
- [ ] Add timeout handling for state transitions
- [ ] Enhanced error handling and recovery

### **Phase 3: Advanced Features (Week 3)**
- [ ] Add `get_job_details()` method with comprehensive state
- [ ] Implement pause reasons and metadata
- [ ] Add metrics for pause/resume operations
- [ ] Integration tests with real Kafka
- [ ] Performance testing and optimization

## 🧪 **Testing Strategy**

### **Unit Tests**
```rust
#[tokio::test]
async fn test_pause_running_job() {
    let server = create_test_server().await;
    server.deploy_job("test_job", "1.0", "SELECT * FROM topic", "topic").await.unwrap();
    
    // Wait for job to be running
    wait_for_job_status(&server, "test_job", JobStatus::Running).await;
    
    // Pause the job
    server.pause_job("test_job").await.unwrap();
    
    // Verify paused state
    let status = server.get_job_status("test_job").await.unwrap();
    assert!(matches!(status.status, JobStatus::Paused { .. }));
}

#[tokio::test]
async fn test_resume_paused_job() {
    // Similar test for resume functionality
}

#[tokio::test]
async fn test_pause_resume_preserves_kafka_state() {
    // Test that Kafka offsets and state are preserved
}

#[tokio::test]
async fn test_multiple_pause_resume_cycles() {
    // Test repeated pause/resume operations
}
```

### **Integration Tests**
- Pause/resume with real Kafka message flow
- State preservation across multiple cycles
- Error recovery scenarios
- Performance impact measurement

## 📊 **Success Criteria**

### **Functional Requirements**
- [ ] Jobs can be paused without losing state
- [ ] Paused jobs stop processing new messages
- [ ] Kafka consumer offsets are preserved during pause
- [ ] Jobs can be resumed and continue from where they left off
- [ ] Multiple pause/resume cycles work correctly
- [ ] Pause/resume operations complete within 5 seconds

### **Performance Requirements**
- [ ] Pause operation completes within 2 seconds
- [ ] Resume operation completes within 3 seconds  
- [ ] No message loss during pause/resume transitions
- [ ] Kafka lag increases predictably while paused
- [ ] Memory usage remains stable during pause

### **Reliability Requirements**
- [ ] State transitions are atomic and consistent
- [ ] Error handling for failed pause/resume operations
- [ ] Recovery from network failures during transitions
- [ ] Proper cleanup if job crashes during pause/resume

## 🚨 **Risks and Mitigations**

### **Technical Risks**
1. **Kafka Consumer State Loss**
   - *Risk*: Consumer group rebalancing during pause
   - *Mitigation*: Implement proper session timeout management

2. **Memory Leaks During Long Pauses**
   - *Risk*: Resources not released during extended pause
   - *Mitigation*: Periodic cleanup and memory monitoring

3. **Race Conditions**
   - *Risk*: Status inconsistencies during rapid pause/resume
   - *Mitigation*: Atomic state transitions with timeouts

### **Operational Risks**
1. **Kafka Lag Buildup**
   - *Risk*: Large lag during extended pause periods
   - *Mitigation*: Lag monitoring and alerts

2. **Complex State Management**
   - *Risk*: Difficult troubleshooting of pause/resume issues
   - *Mitigation*: Comprehensive logging and status reporting

## 🔄 **Backwards Compatibility**

### **API Changes**
- All existing methods remain unchanged
- New methods are additive only
- `JobStatus` enum extended with new `Paused` variant
- Existing status values unchanged

### **Migration Path**
- Feature is opt-in (existing jobs unaffected)
- Gradual rollout possible
- Fallback to stop/start if pause fails

## 📈 **Metrics and Monitoring**

### **New Metrics**
- `jobs_paused_total` - Counter of pause operations
- `jobs_resumed_total` - Counter of resume operations
- `job_pause_duration_seconds` - Histogram of pause durations
- `job_kafka_lag_while_paused` - Gauge of Kafka lag during pause
- `job_pause_resume_errors_total` - Counter of failed operations

### **Dashboards**
- Job pause/resume status overview
- Kafka lag monitoring for paused jobs
- Pause duration and frequency analytics
- Error rates and troubleshooting

## 🎯 **Definition of Done**

- [ ] All pause/resume functionality implemented and tested
- [ ] Unit test coverage > 95% for new code
- [ ] Integration tests passing with real Kafka
- [ ] Performance tests meet success criteria
- [ ] Documentation updated (API docs, operator guide)
- [ ] Metrics and monitoring implemented
- [ ] Code review completed
- [ ] Feature flag for gradual rollout implemented

## 👥 **Stakeholders**

- **Development Team**: Implementation and testing
- **Operations Team**: Requirements review and acceptance testing  
- **Product Team**: Feature prioritization and user experience
- **QA Team**: Comprehensive testing and validation

---

**Created**: 2025-01-08  
**Last Updated**: 2025-01-08  
**Next Review**: 2025-01-15