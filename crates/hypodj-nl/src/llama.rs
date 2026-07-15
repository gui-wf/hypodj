//! The concrete local constrained-decode backend (feature = "llm-llama").
//!
//! Deployment/ops artifact: `llama-cpp-sys` compiles llama.cpp via bindgen (needs
//! libclang + a C++ toolchain), and a quantized (8-bit) GGUF model FILE is a
//! RUNTIME artifact, not a repo/CI dependency. This module is the wiring point:
//! it implements [`crate::llm::LlmBackend`] by loading the model once and running
//! a GBNF-constrained decode. Until a model path is configured it returns a clear
//! error, so the hybrid degrades to Rules-only + a loud NotUnderstood (the model
//! is never REQUIRED). The SEAM, schema, GBNF, and output parse it depends on are
//! all model-free and tested under `feature = "llm"`.

use std::path::PathBuf;

// Declare the intended runtime dependency explicitly (the real integration binds
// llama.cpp's `json-schema-to-grammar` + a constrained sampler here).
use llama_cpp_2 as _;

use crate::llm::LlmBackend;

/// A llama.cpp-backed constrained-decode backend. Holds the model path; the model
/// is loaded lazily on first `generate`.
pub struct LlamaBackend {
    model_path: PathBuf,
}

impl LlamaBackend {
    /// Construct from a GGUF model path (a runtime/ops artifact). If the file is
    /// absent, the daemon should inject `None` (Rules-only) rather than this.
    pub fn new(model_path: PathBuf) -> Self {
        Self { model_path }
    }
}

impl LlmBackend for LlamaBackend {
    fn generate(&self, _prompt: &str, _gbnf: &str) -> Result<String, String> {
        // TODO(ops): load `self.model_path` via llama-cpp-2, install the GBNF as a
        // grammar sampler, decode, and return the JSON string. Kept as an explicit
        // fail (never a fabricated plan) until a model is wired at deploy time.
        Err(format!(
            "llama backend not wired: no constrained decode for model at {}",
            self.model_path.display()
        ))
    }
}
