//! WasmOperator: bridges WASM Guest UDFs with the StreamCrab Operator trait.
//!
//! Host manages all state (I_WASM_2). Guest executes pure computation (I_WASM_1/3).

use anyhow::Result;

use streamcrab_core::operator_chain::Operator;
use streamcrab_core::state::KeyedStateBackend;

use crate::abi::{WasmInput, WasmOutput, WasmRecord};
use crate::runtime::{WasmRuntime, WasmRuntimeConfig};

/// Operator that delegates processing to a WASM Guest UDF.
///
/// Generic over `S: KeyedStateBackend` because `KeyedStateBackend` has generic
/// methods and is not dyn-compatible.
///
/// Input/output type is `Vec<u8>` (opaque bytes). The WASM Guest defines
/// the actual schema via its own serde types.
pub struct WasmOperator<S: KeyedStateBackend> {
    runtime: WasmRuntime,
    wasm_bytes: Vec<u8>,
    config: WasmRuntimeConfig,
    state_backend: Option<S>,
    /// State name used in the KeyedStateBackend for this operator's UDF state.
    state_name: String,
    /// Pending WASM module bytes to hot-swap at the next checkpoint barrier.
    pending_wasm_bytes: Option<Vec<u8>>,
}

impl<S: KeyedStateBackend> WasmOperator<S> {
    /// Create a new WasmOperator from WASM module bytes.
    pub fn new(
        wasm_bytes: Vec<u8>,
        config: WasmRuntimeConfig,
        state_backend: Option<S>,
    ) -> Result<Self> {
        let runtime = WasmRuntime::new(&wasm_bytes, config.clone())?;
        Ok(Self {
            runtime,
            wasm_bytes,
            config,
            state_backend,
            state_name: "__wasm_udf_state".to_string(),
            pending_wasm_bytes: None,
        })
    }

    /// Read current state bytes from the Host StateBackend.
    fn read_state(&self) -> Result<Option<Vec<u8>>> {
        match &self.state_backend {
            Some(backend) => backend.get_value::<Vec<u8>>(&self.state_name),
            None => Ok(None),
        }
    }

    /// Write updated state bytes to the Host StateBackend.
    fn write_state(&mut self, state: Vec<u8>) -> Result<()> {
        if let Some(backend) = &mut self.state_backend {
            backend.put_value(&self.state_name, state)?;
        }
        Ok(())
    }

    /// Schedule a WASM module update. The swap happens at the next checkpoint barrier.
    pub fn schedule_update(&mut self, new_wasm_bytes: Vec<u8>) {
        self.pending_wasm_bytes = Some(new_wasm_bytes);
    }

    /// Returns true if a WASM module update is pending.
    pub fn has_pending_update(&self) -> bool {
        self.pending_wasm_bytes.is_some()
    }

    /// Set the current processing key on the state backend.
    pub fn set_current_key(&mut self, key: Vec<u8>) {
        if let Some(backend) = &mut self.state_backend {
            backend.set_current_key(key);
        }
    }

    /// Process a single record through the WASM Guest.
    fn process_record(&mut self, record: &[u8]) -> Result<WasmOutput> {
        let state = self.read_state()?;

        let input = WasmInput {
            record: WasmRecord {
                key: vec![],
                value: record.to_vec(),
                timestamp: None,
            },
            state,
        };

        let output = self.runtime.call(&input)?;

        // Persist updated state if the Guest returned new state
        if let Some(new_state) = &output.state {
            self.write_state(new_state.clone())?;
        }

        Ok(output)
    }
}

impl<S: KeyedStateBackend> Operator<Vec<u8>> for WasmOperator<S> {
    type OUT = Vec<u8>;

    fn process_batch(&mut self, input: &[Vec<u8>], output: &mut Vec<Self::OUT>) -> Result<()> {
        for record in input {
            let wasm_output = self.process_record(record)?;
            for out_record in &wasm_output.records {
                output.push(out_record.value.clone());
            }
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        // WASM Instance is NOT snapshotted (I_WASM_2: stateless).
        // Only the Host StateBackend state matters.
        match &self.state_backend {
            Some(backend) => backend.snapshot(),
            None => Ok(Vec::new()),
        }
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        // Restore Host state, then rebuild WASM Instance (I_WASM_2).
        if let Some(backend) = &mut self.state_backend {
            backend.restore(data)?;
        }
        // Fresh Instance after restore — guaranteed clean
        self.runtime = WasmRuntime::new(&self.wasm_bytes, self.config.clone())?;
        Ok(())
    }

    fn on_checkpoint_barrier(&mut self, _checkpoint_id: u64) -> Result<()> {
        // Flush state first
        if let Some(backend) = &mut self.state_backend {
            backend.flush()?;
        }
        // Hot-swap WASM module if update is pending (I_WASM_2: Instance is disposable)
        if let Some(new_bytes) = self.pending_wasm_bytes.take() {
            self.runtime = WasmRuntime::new(&new_bytes, self.config.clone())?;
            self.wasm_bytes = new_bytes;
            tracing::info!("WASM module hot-swapped at checkpoint barrier");
        } else if self.config.rebuild_on_checkpoint {
            // I_WASM_2: optionally rebuild Instance on checkpoint for strongest leak prevention
            self.runtime.rebuild()?;
        }
        Ok(())
    }
}
