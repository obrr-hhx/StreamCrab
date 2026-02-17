use streamcrab_core::types::StreamData;

use crate::datastream::DataStream;

/// The entry point for building a stream processing job.
///
/// Create an environment, add sources via [`from_iter`](Self::from_iter),
/// chain transformations on the returned [`DataStream`].
pub struct StreamExecutionEnvironment;

impl StreamExecutionEnvironment {
    /// Create a new execution environment for a job with the given name.
    pub fn new(_job_name: &str) -> Self {
        Self
    }

    /// Add a source that produces elements from an iterator.
    ///
    /// Returns a [`DataStream`] that can be transformed with [`key_by`](DataStream::key_by).
    pub fn from_iter<T, I>(&self, iter: I) -> DataStream<T>
    where
        T: StreamData + std::fmt::Debug + Send + 'static,
        I: IntoIterator<Item = T> + 'static,
    {
        let source_data: Vec<T> = iter.into_iter().collect();
        DataStream { source_data }
    }
}
