use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => formatter.write_str("INFO"),
            Self::Warning => formatter.write_str("WARN"),
            Self::Error => formatter.write_str("ERROR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub sequence: u64,
    pub elapsed: Duration,
    pub level: LogLevel,
    pub message: String,
}

pub struct LogBuffer {
    entries: VecDeque<LogEntry>,
    next_sequence: u64,
    started_at: Instant,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            next_sequence: 1,
            started_at: Instant::now(),
            capacity,
        }
    }

    pub fn push(&mut self, level: LogLevel, message: impl Into<String>) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(LogEntry {
            sequence: self.next_sequence,
            elapsed: self.started_at.elapsed(),
            level,
            message: message.into(),
        });
        self.next_sequence += 1;
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn entries(&self) -> &VecDeque<LogEntry> {
        &self.entries
    }
}
