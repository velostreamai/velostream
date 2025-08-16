use super::record::StreamRecord;
use crate::ferris::sql::ast::StreamingQuery;

#[derive(Debug)]
pub enum ExecutionMessage {
    StartJob {
        job_id: String,
        query: StreamingQuery,
    },
    StopJob {
        job_id: String,
    },
    ProcessRecord {
        stream_name: String,
        record: StreamRecord,
    },
    QueryResult {
        query_id: String,
        result: StreamRecord,
    },
}

#[derive(Debug, Clone)]
pub struct HeaderMutation {
    pub operation: HeaderOperation,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone)]
pub enum HeaderOperation {
    Set,
    Remove,
}
