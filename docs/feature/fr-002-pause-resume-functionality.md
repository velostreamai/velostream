# FR-002: Pause/Resume Job Functionality

**Status**: In Progress - Partial Implementation
**Priority**: Medium
**Complexity**: High
**Estimated Effort**: 1 week (remaining)

## 📋 **Summary**

Complete the pause/resume functionality for StreamJobServer jobs. The codebase already has AST support, parser support, and a partial implementation of `pause_job()`. This feature requires completing the `resume_job()` method and fixing the pause mechanism to truly suspend jobs rather than aborting them.

## 📊 **Current Implementation Status**

### **What's Already Implemented** ✅
- `JobStatus` enum with `Paused` variant
- `StreamingQuery::PauseJob` and `StreamingQuery::ResumeJob` AST variants
- SQL parser support for `PAUSE JOB <name>` and `RESUME JOB <name>` commands
- `StreamJobServer::pause_job()` method (partially working - uses task abort)
- Parser-level unit tests for pause/resume commands

### **What's Missing** ❌
- `StreamJobServer::resume_job()` method (critical missing piece)
- Proper pause mechanism (currently aborts task instead of suspending)
- `JobProcessor` integration (has stubs that need wiring)
- Server-level unit tests for `resume_job()`
- State preservation during pause/resume cycles

### **Architecture Notes**
- **Server Class**: `StreamJobServer` (not MultiJobSqlServer as in original spec)
- **Location**: `src/velostream/server/stream_job_server.rs`
- **Current pause approach**: Uses `shutdown_sender` channel to abort task
- **Issue**: Task abortion prevents clean resume - need graceful pause instead

## 🎯 **Problem Statement**

### **Current Limitation**
- Jobs have pause capability but **no resume functionality**
- Pause mechanism aborts the task entirely, preventing true resume
- Paused jobs lose runtime state and Kafka consumer context
- Operators cannot suspend jobs for maintenance and resume them afterward

### **Use Cases**
1. **Maintenance Windows**: Pause jobs during system maintenance without losing state
2. **Resource Management**: Temporarily free resources for high-priority tasks
3. **Debugging**: Pause suspect jobs for investigation while preserving context
4. **Gradual Rollouts**: Pause old jobs before starting new versions
5. **Cost Optimization**: Pause non-critical jobs during peak pricing periods

## 🚀 **Proposed Solution**

### **Complete API Methods**

```rust
impl StreamJobServer {
    // ✅ EXISTING: Pause a running job (needs fix - currently aborts)
    pub async fn pause_job(&self, name: &str) -> Result<(), SqlError> {
        // Currently implemented at stream_job_server.rs:1033
        // Issue: Uses shutdown_sender which aborts task completely
    }

    // ❌ MISSING: Resume a paused job (needs implementation)
    pub async fn resume_job(&self, name: &str) -> Result<(), SqlError> {
        // Find paused job, restart execution, change status to Running
        // Currently: NO IMPLEMENTATION
    }

    /// Get detailed job state including pause information (future enhancement)
    pub async fn get_job_details(&self, name: &str) -> Option<JobDetails> {
        // Return comprehensive job state including pause history
    }
}
```

### **Existing Code Locations**

| Component | File | Line | Status |
|-----------|------|------|--------|
| `pause_job()` implementation | `src/velostream/server/stream_job_server.rs` | 1033-1051 | ⚠️ Partial |
| `JobStatus::Paused` variant | `src/velostream/sql/ast.rs` | 62-65 | ✅ Complete |
| `StreamingQuery::PauseJob` | `src/velostream/sql/ast.rs` | 213-216 | ✅ Complete |
| `StreamingQuery::ResumeJob` | `src/velostream/sql/ast.rs` | 221-224 | ✅ Complete |
| Parser for PAUSE/RESUME | `src/velostream/sql/parser.rs` | — | ✅ Complete |
| `JobProcessor` stubs | `src/velostream/sql/execution/processors/job.rs` | 117-192 | ⚠️ Stub Only |
| Parser tests | `tests/unit/sql/lifecycle_test.rs` | — | ✅ Complete |

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

### **Phase 1: Complete Missing `resume_job()` Method (3 days)**
Since most components already exist, focus on the critical missing piece:

- [ ] Implement `StreamJobServer::resume_job()` method
  - Find paused job by name in `self.jobs` map
  - Verify job status is `JobStatus::Paused`
  - Change status to `JobStatus::Running`
  - Update `updated_at` timestamp
  - Return success/error result

- [ ] Fix pause mechanism to support true resume
  - Current issue: `pause_job()` sends shutdown signal that aborts task
  - Need graceful pause that suspends message processing but preserves consumer state
  - Option A: Add separate pause flag (don't abort the task)
  - Option B: Re-deploy job on resume (simpler but loses Kafka state)

- [ ] Add server-level unit tests
  - `test_resume_job_basic()` - Resume after pause
  - `test_pause_resume_state_transitions()` - Status changes
  - `test_resume_job_not_found()` - Error handling

### **Phase 2: Wire JobProcessor Integration (2 days)**
- [ ] Replace `JobProcessor::process_pause_job()` stub
  - Call `StreamJobServer::pause_job()`
  - Return proper response to client

- [ ] Replace `JobProcessor::process_resume_job()` stub
  - Call `StreamJobServer::resume_job()`
  - Return proper response to client

- [ ] Integration tests
  - SQL `PAUSE JOB` command execution
  - SQL `RESUME JOB` command execution

### **Phase 3: Enhanced Features (Optional - Future)**
- [ ] Add `get_job_details()` with pause history
- [ ] Implement pause reasons and metadata
- [ ] Add metrics for pause/resume operations
- [ ] Kafka state preservation across pause/resume
- [ ] Performance testing and optimization

## 🧪 **Testing Strategy**

### **Unit Tests - Already Passing** ✅
```rust
// Location: tests/unit/sql/lifecycle_test.rs

#[test]
fn test_pause_job_basic() {
    // ✅ PASSES - PAUSE JOB parsing works
}

#[test]
fn test_resume_job_basic() {
    // ✅ PASSES - RESUME JOB parsing works
}

#[test]
fn test_case_insensitive_lifecycle_commands() {
    // ✅ PASSES - Case-insensitive parsing works
}
```

### **Unit Tests - Need to Implement** ❌
```rust
// Location: tests/unit/server/stream_job_server_test.rs (or similar)

#[tokio::test]
async fn test_pause_job_changes_status() {
    let server = create_test_server().await;
    let job = deploy_test_job(&server, "test_job").await;

    // Pause the job
    server.pause_job("test_job").await.unwrap();

    // Verify status changed to Paused
    let jobs = server.list_jobs().await.unwrap();
    let job_status = jobs.iter().find(|j| j.name == "test_job").unwrap();
    assert!(matches!(job_status.status, JobStatus::Paused));
}

#[tokio::test]
async fn test_resume_job_basic() {
    // THIS IS THE CRITICAL MISSING TEST
    let server = create_test_server().await;
    deploy_test_job(&server, "test_job").await;

    // Pause then resume
    server.pause_job("test_job").await.unwrap();
    server.resume_job("test_job").await.unwrap();

    // Verify status changed back to Running
    let jobs = server.list_jobs().await.unwrap();
    let job_status = jobs.iter().find(|j| j.name == "test_job").unwrap();
    assert!(matches!(job_status.status, JobStatus::Running));
}

#[tokio::test]
async fn test_resume_nonpaused_job_errors() {
    let server = create_test_server().await;
    deploy_test_job(&server, "test_job").await;

    // Try to resume a running job (should error)
    let result = server.resume_job("test_job").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_resume_missing_job_errors() {
    let server = create_test_server().await;

    // Try to resume non-existent job
    let result = server.resume_job("missing_job").await;
    assert!(result.is_err());
}
```

### **Integration Tests - Job Execution** ⏳
- Execute `PAUSE JOB` SQL command and verify pause
- Execute `RESUME JOB` SQL command and verify resume
- Verify JobProcessor wiring works correctly
- Test error cases (invalid job names, wrong states)

## 📊 **Success Criteria**

### **Phase 1 Functional Requirements (Critical Path)**
- [x] Parser supports PAUSE JOB and RESUME JOB syntax
- [x] JobStatus enum has Paused variant
- [x] AST has PauseJob and ResumeJob variants
- [ ] `StreamJobServer::pause_job()` changes job status correctly
- [ ] `StreamJobServer::resume_job()` method exists and works
- [ ] Resume changes status from Paused to Running
- [ ] Error handling: Resume non-existent job returns error
- [ ] Error handling: Resume non-paused job returns error

### **Phase 2 Integration Requirements**
- [ ] `JobProcessor::process_pause_job()` calls server method
- [ ] `JobProcessor::process_resume_job()` calls server method
- [ ] SQL `PAUSE JOB <name>` command executes successfully
- [ ] SQL `RESUME JOB <name>` command executes successfully

### **Phase 3+ Performance & State Requirements (Future)**
- [ ] Pause/resume complete within 5 seconds
- [ ] Jobs preserve Kafka consumer offsets during pause
- [ ] Jobs can resume from correct offset
- [ ] Multiple pause/resume cycles work without data loss
- [ ] Kafka lag monitoring accurate while paused

### **Reliability Requirements (Current)**
- [ ] Status transitions are consistent
- [ ] Proper error messages for invalid operations
- [ ] Server handles concurrent pause/resume requests safely

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

### **Phase 1 - Core Functionality** (Current Focus)
- [ ] `StreamJobServer::resume_job()` method implemented
- [ ] Unit tests for pause/resume state transitions pass
- [ ] Unit tests for error cases pass
- [ ] Code compiles and passes CI
- [ ] Code review completed

### **Phase 2 - Integration** (Next)
- [ ] JobProcessor methods wire to server implementation
- [ ] SQL `PAUSE JOB` and `RESUME JOB` command execution works
- [ ] Integration tests pass
- [ ] Error handling and logging complete

### **Phase 3+ - Polish & Enhancement** (Future)
- [ ] Performance tests meet success criteria
- [ ] Documentation updated (API docs, operator guide)
- [ ] Metrics and monitoring implemented
- [ ] Feature flag for gradual rollout (if needed)
- [ ] Kafka state preservation verified

## 👥 **Stakeholders**

- **Development Team**: Implementation and testing
- **Operations Team**: Requirements review and acceptance testing  
- **Product Team**: Feature prioritization and user experience
- **QA Team**: Comprehensive testing and validation

---

**Created**: 2025-01-08
**Last Updated**: 2025-10-29
**Last Reviewed**: 2025-10-29 (Implementation Status Audit Completed)
**Next Review**: After Phase 1 completion

## 📝 **Implementation Status Audit (2025-10-29)**

### **Key Findings**
1. **Server**: `StreamJobServer` in `src/velostream/server/stream_job_server.rs` (not MultiJobSqlServer)
2. **Partially Implemented**: `pause_job()` exists but has architectural issues (uses task abort)
3. **Completely Missing**: `resume_job()` method (main blocker)
4. **AST Ready**: PauseJob and ResumeJob variants already in AST
5. **Parser Ready**: PAUSE JOB and RESUME JOB parsing works
6. **Stubs Only**: JobProcessor has placeholder implementations

### **Critical Path to Completion**
1. Implement `resume_job()` method (~1 day)
2. Fix pause mechanism for true suspension (~1 day)
3. Wire JobProcessor integration (~1 day)
4. Add comprehensive tests (~1 day)
5. Total: ~1 week estimated effort (revised down from 2-3 weeks)