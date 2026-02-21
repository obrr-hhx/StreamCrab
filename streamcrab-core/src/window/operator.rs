use super::*;

// ── WindowOperator ────────────────────────────────────────────────────────────

/// Core windowing operator.
///
/// Accepts [`StreamElement<T>`] items (records + watermarks) and emits
/// [`StreamElement<OUT>`] items when windows fire.
///
/// # Processing model
///
/// - **Records**: assigned to one or more windows by the `WindowAssigner`,
///   then buffered in memory per `(key, window)`.
/// - **Watermarks**: trigger all windows whose `max_timestamp < watermark`;
///   fired windows are evaluated by `WindowFunction` and then purged.
///   The watermark is re-emitted downstream unchanged.
pub struct WindowOperator<K, T, OUT, KF, TF, WA, TR, WFN>
where
    K: StreamData,
    T: StreamData,
    OUT: StreamData,
    KF: Fn(&T) -> K + Send,
    TF: Fn(&T) -> EventTime + Send,
    WA: WindowAssigner<T>,
    TR: Trigger<T, TimeWindow>,
    WFN: WindowFunction<K, T, OUT>,
{
    key_fn: KF,
    timestamp_fn: TF,
    assigner: WA,
    trigger: TR,
    window_fn: WFN,
    /// Buffered elements: (key_bytes, window) -> (original_key, elements).
    /// key_bytes is used as the HashMap key to allow O(1) lookup.
    /// The original key is kept alongside to avoid deserialization.
    buffers: HashMap<(Vec<u8>, TimeWindow), (K, Vec<T>)>,
    /// Event-time timers used to drive trigger callbacks.
    timer_service: TimerService,
    current_watermark: EventTime,
    _phantom: PhantomData<OUT>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WindowOperatorSnapshot<K, T> {
    buffered_entries: Vec<(Vec<u8>, TimeWindow, K, Vec<T>)>,
    timer_service: TimerService,
    current_watermark: EventTime,
}

impl<K, T, OUT, KF, TF, WA, TR, WFN> WindowOperator<K, T, OUT, KF, TF, WA, TR, WFN>
where
    K: StreamData,
    T: StreamData,
    OUT: StreamData,
    KF: Fn(&T) -> K + Send,
    TF: Fn(&T) -> EventTime + Send,
    WA: WindowAssigner<T>,
    TR: Trigger<T, TimeWindow>,
    WFN: WindowFunction<K, T, OUT>,
{
    /// Create a new `WindowOperator`.
    ///
    /// - `key_fn`: extracts the grouping key from each element
    /// - `timestamp_fn`: extracts the event-time timestamp from each element
    /// - `assigner`: assigns windows (Tumbling / Sliding / Session / Global)
    /// - `trigger`: controls when windows fire/purge
    /// - `window_fn`: computes the result when a window fires
    pub fn new(key_fn: KF, timestamp_fn: TF, assigner: WA, trigger: TR, window_fn: WFN) -> Self {
        Self {
            key_fn,
            timestamp_fn,
            assigner,
            trigger,
            window_fn,
            buffers: HashMap::new(),
            timer_service: TimerService::new(),
            current_watermark: EVENT_TIME_MIN,
            _phantom: PhantomData,
        }
    }

    /// Snapshot buffered window state and timers.
    pub fn snapshot_state(&self) -> Result<Vec<u8>> {
        let buffered_entries = self
            .buffers
            .iter()
            .map(|((key_bytes, window), (key, elements))| {
                (
                    key_bytes.clone(),
                    window.clone(),
                    key.clone(),
                    elements.clone(),
                )
            })
            .collect();

        let snapshot = WindowOperatorSnapshot {
            buffered_entries,
            timer_service: self.timer_service.clone(),
            current_watermark: self.current_watermark,
        };
        Ok(bincode::serialize(&snapshot)?)
    }

    /// Restore buffered window state and timers.
    pub fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            self.buffers.clear();
            self.timer_service = TimerService::new();
            self.current_watermark = EVENT_TIME_MIN;
            return Ok(());
        }

        let snapshot: WindowOperatorSnapshot<K, T> = bincode::deserialize(data)?;
        self.buffers.clear();
        for (key_bytes, window, key, elements) in snapshot.buffered_entries {
            self.buffers.insert((key_bytes, window), (key, elements));
        }
        self.timer_service = snapshot.timer_service;
        self.current_watermark = snapshot.current_watermark;
        Ok(())
    }

    fn register_event_time_timer(&mut self, map_key: &(Vec<u8>, TimeWindow)) -> Result<()> {
        let timer_key = bincode::serialize(map_key)?;
        self.timer_service
            .register(timer_key, map_key.1.max_timestamp());
        Ok(())
    }

    fn delete_event_time_timer(&mut self, map_key: &(Vec<u8>, TimeWindow)) -> Result<()> {
        let timer_key = bincode::serialize(map_key)?;
        self.timer_service
            .delete(&timer_key, map_key.1.max_timestamp());
        Ok(())
    }

    fn apply_trigger_result(
        &mut self,
        map_key: (Vec<u8>, TimeWindow),
        trigger_result: TriggerResult,
        output: &mut Vec<StreamElement<OUT>>,
    ) -> Result<()> {
        if trigger_result.is_fire() {
            if let Some((orig_key, elements)) = self.buffers.remove(&map_key) {
                let mut out_values: Vec<OUT> = Vec::new();
                self.window_fn
                    .apply(&orig_key, &map_key.1, &elements, &mut out_values);
                for val in out_values {
                    output.push(StreamElement::timestamped_record(
                        val,
                        map_key.1.max_timestamp(),
                    ));
                }

                if !trigger_result.is_purge() {
                    self.buffers.insert(map_key, (orig_key, elements));
                } else {
                    self.delete_event_time_timer(&map_key)?;
                }
            }
            return Ok(());
        }

        if trigger_result.is_purge() {
            self.buffers.remove(&map_key);
            self.delete_event_time_timer(&map_key)?;
        }
        Ok(())
    }

    /// Fire due event-time timers at `event_time`.
    ///
    /// This is the explicit timer callback path:
    /// - drains due timers
    /// - calls `trigger.on_event_time`
    /// - applies fire/purge and emits window results
    pub fn on_timer(&mut self, event_time: EventTime) -> Result<Vec<StreamElement<OUT>>> {
        self.current_watermark = self.current_watermark.max(event_time);

        let mut trigger_results: Vec<((Vec<u8>, TimeWindow), TriggerResult)> = Vec::new();
        for (timer_key, fire_at) in self.timer_service.drain_due(event_time) {
            let map_key: (Vec<u8>, TimeWindow) = bincode::deserialize(&timer_key)?;
            let result = self.trigger.on_event_time(fire_at, &map_key.1);
            trigger_results.push((map_key, result));
        }

        let mut output: Vec<StreamElement<OUT>> = Vec::new();
        for (map_key, trigger_result) in trigger_results {
            self.apply_trigger_result(map_key, trigger_result, &mut output)?;
        }
        Ok(output)
    }

    /// Process one stream element and return any window results produced.
    ///
    /// - Records are buffered; output is empty unless a trigger fires immediately.
    /// - Watermarks advance event time, firing all expired windows and then
    ///   re-emitting the watermark so downstream operators stay in sync.
    pub fn process(&mut self, elem: StreamElement<T>) -> Result<Vec<StreamElement<OUT>>> {
        match elem {
            StreamElement::Record(rec) => {
                let key = (self.key_fn)(&rec.value);
                let key_bytes = bincode::serialize(&key)?;
                // Use timestamp from record, fall back to timestamp_fn.
                let ts = rec
                    .timestamp
                    .unwrap_or_else(|| (self.timestamp_fn)(&rec.value));
                let windows = self.assigner.assign_windows(&rec.value, ts);
                let mut output = Vec::new();
                for window in windows {
                    let map_key = (key_bytes.clone(), window.clone());
                    let entry = self
                        .buffers
                        .entry(map_key.clone())
                        .or_insert_with(|| (key.clone(), Vec::new()));
                    entry.1.push(rec.value.clone());

                    // Default event-time semantics: register timer at window.max_timestamp().
                    self.register_event_time_timer(&map_key)?;

                    let trigger_result = self.trigger.on_element(&rec.value, ts, &window);
                    self.apply_trigger_result(map_key, trigger_result, &mut output)?;
                }
                Ok(output)
            }

            StreamElement::Watermark(wm) => {
                self.current_watermark = wm.timestamp;
                let mut output = self.on_timer(wm.timestamp)?;

                // Re-emit the watermark downstream so the pipeline keeps advancing.
                output.push(StreamElement::Watermark(wm));
                Ok(output)
            }

            StreamElement::CheckpointBarrier(b) => Ok(vec![StreamElement::CheckpointBarrier(b)]),

            StreamElement::End => Ok(vec![StreamElement::End]),
        }
    }

    /// Trigger processing-time callbacks for all active windows.
    pub fn on_processing_time(
        &mut self,
        processing_time: EventTime,
    ) -> Result<Vec<StreamElement<OUT>>> {
        let trigger_results: Vec<((Vec<u8>, TimeWindow), TriggerResult)> = self
            .buffers
            .keys()
            .map(|map_key| {
                (
                    map_key.clone(),
                    self.trigger.on_processing_time(processing_time, &map_key.1),
                )
            })
            .collect();

        let mut output: Vec<StreamElement<OUT>> = Vec::new();
        for (map_key, trigger_result) in trigger_results {
            self.apply_trigger_result(map_key, trigger_result, &mut output)?;
        }
        Ok(output)
    }

    /// Return the number of currently buffered (key, window) pairs.
    pub fn buffered_window_count(&self) -> usize {
        self.buffers.len()
    }
}
