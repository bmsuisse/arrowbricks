mod diagnostics;
pub mod render;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_schema::Schema;
use serde_json::Value;

use crate::client::{ApiError, DbClient, Protocol};
use crate::pipeline;
use render::AgentRenderer;
pub use render::OutputFormat;

#[derive(Clone, Copy)]
pub struct AgentOptions {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_cell_chars: usize,
    pub total_timeout_s: f64,
    pub format: OutputFormat,
}

impl Default for AgentOptions {
    fn default() -> Self {
        Self {
            max_rows: 1000,
            max_bytes: 10_000,
            max_cell_chars: 500,
            total_timeout_s: 60.0,
            format: OutputFormat::default(),
        }
    }
}

impl AgentOptions {
    pub fn validate(&self) -> Result<Duration, &'static str> {
        if self.max_rows == 0 || self.max_rows >= isize::MAX as usize {
            return Err("max_rows must be positive and less than the platform's maximum signed integer");
        }
        if self.max_bytes < 256 || self.max_bytes > isize::MAX as usize {
            return Err("max_bytes must be at least 256 and fit the platform's maximum signed integer");
        }
        if self.max_cell_chars == 0 || self.max_cell_chars > isize::MAX as usize {
            return Err("max_cell_chars must be positive and fit the platform's maximum signed integer");
        }
        Duration::try_from_secs_f64(self.total_timeout_s)
            .ok()
            .filter(|duration| std::time::Instant::now().checked_add(*duration).is_some())
            .ok_or("total_timeout_s must be a finite, non-negative, representable number of seconds")
    }
}

type CancelHook = Box<dyn FnOnce(bool) + Send>;

#[derive(Default)]
struct CancelOnDrop {
    hook: Mutex<Option<CancelHook>>,
    interrupted: Arc<AtomicBool>,
}

impl CancelOnDrop {
    fn fire(&self, timeout: bool) {
        self.interrupted.store(true, Ordering::Relaxed);
        if let Some(hook) = self.hook.lock().unwrap().take() {
            hook(timeout);
        }
    }
    fn defuse(&self) {
        self.hook.lock().unwrap().take();
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.fire(false);
    }
}

struct AbortBlockingOnDrop(tokio::task::AbortHandle);

impl Drop for AbortBlockingOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct AgentResult(pipeline::ResultStream);

impl std::ops::Deref for AgentResult {
    type Target = pipeline::ResultStream;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for AgentResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AgentResult {
    fn finish(&mut self, outcome: &'static str) {
        self.0.reporter.finish(outcome, &self.0.stats);
    }
}

impl Drop for AgentResult {
    fn drop(&mut self) {
        let outcome = self.0.stats.pending_outcome().unwrap_or("cancelled");
        self.finish(outcome);
    }
}

pub async fn query(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
    options: AgentOptions,
) -> String {
    let duration = match options.validate() {
        Ok(duration) => duration,
        Err(message) => {
            return diagnostics::error_json(&ApiError::permanent(message), options.max_bytes.max(256), false);
        }
    };
    let deadline = tokio::time::Instant::now() + duration;
    let cancellation = CancelOnDrop::default();
    let work = async {
        let mut stream = if client.protocol == Protocol::Thrift {
            pipeline::execute_lazy_thrift_with_concurrency(
                client.clone(),
                statement,
                catalog,
                schema,
                parameters,
                Some(1),
            )
            .await?
        } else {
            pipeline::execute_lazy_with_concurrency(client.clone(), statement, catalog, schema, parameters, Some(1))
                .await?
        };
        stream.reporter.concurrency_used = Some(1);
        stream.defer_completion = true;
        *cancellation.hook.lock().unwrap() = Some(Box::new(pipeline::cancel_hook(
            client,
            stream.cancel_handle.clone(),
            stream.stats.clone(),
        )));
        let mut stream = AgentResult(stream);
        let result = async {
            let mut renderer = None;
            let mut finished = false;
            let mut lookahead_found = false;
            let mut unrendered_rows = false;
            while !finished {
                let want = renderer
                    .as_ref()
                    .map_or(128, |r: &AgentRenderer| {
                        options.max_rows.saturating_sub(r.rows()).min(128)
                    })
                    .min(options.max_rows);
                let (batches, schema) = stream.fetchmany_arrow(want).await?;
                let interrupted = cancellation.interrupted.clone();
                let mut render_task = tokio::task::spawn_blocking(move || {
                    let mut renderer = match renderer {
                        Some(renderer) => renderer,
                        None => AgentRenderer::new_with_interrupt(
                            schema.unwrap_or_else(|| Arc::new(Schema::empty())),
                            options.max_rows,
                            options.max_bytes,
                            options.max_cell_chars,
                            options.format,
                            deadline.into_std(),
                            interrupted,
                        )?,
                    };
                    let mut stopped = false;
                    let mut omitted = false;
                    for (index, batch) in batches.iter().enumerate() {
                        let before = renderer.rows();
                        if !renderer.push_batch(batch)? {
                            omitted = renderer.rows() - before < batch.num_rows()
                                || batches[index + 1..].iter().any(|batch| batch.num_rows() > 0);
                            stopped = true;
                            break;
                        }
                    }
                    Ok::<_, ApiError>((renderer, stopped, omitted))
                });
                let abort_render = AbortBlockingOnDrop(render_task.abort_handle());
                let (rendered, stopped, omitted) = (&mut render_task)
                    .await
                    .map_err(|error| ApiError::permanent(format!("agent rendering worker failed: {error}")))??;
                drop(abort_render);
                finished |= stopped;
                unrendered_rows |= omitted;
                renderer = Some(rendered);
                if renderer.as_ref().unwrap().rows() >= options.max_rows {
                    // One row proves a larger result; only exhaustion proves an exact total.
                    let (lookahead, _) = stream.fetchmany_arrow(1).await?;
                    lookahead_found = lookahead.iter().any(|batch| batch.num_rows() > 0);
                    finished = true;
                }
                if stream.exhausted && stream.pending_rows == 0 {
                    finished = true;
                }
            }
            let exhausted = !lookahead_found && !unrendered_rows && stream.exhausted && stream.pending_rows == 0;
            let output = renderer.unwrap().finish(exhausted);
            if tokio::time::Instant::now() >= deadline {
                return Err(ApiError::permanent("agent total deadline elapsed during rendering"));
            }
            if exhausted {
                cancellation.defuse();
            } else {
                stream.finish("success");
                cancellation.fire(false);
            }
            Ok::<_, ApiError>(output)
        }
        .await;
        stream.finish(match &result {
            Ok(_) => "success",
            Err(_) if tokio::time::Instant::now() >= deadline => "timeout",
            Err(_) => "error",
        });
        result
    };
    tokio::pin!(work);
    tokio::select! {
        biased;
        () = tokio::time::sleep_until(deadline) => {
            cancellation.fire(true);
            diagnostics::error_json(&ApiError::permanent("Query exceeded total_timeout_s deadline"), options.max_bytes, true)
        }
        result = &mut work => match result {
            Ok(output) => output,
            Err(error) => {
                let timeout = tokio::time::Instant::now() >= deadline;
                cancellation.fire(timeout);
                diagnostics::error_json(&error, options.max_bytes, timeout)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Events(Mutex<Vec<&'static str>>);

    impl crate::client::EventSink for Events {
        fn on_event(&self, stats: crate::client::QueryStatsData) {
            self.0.lock().unwrap().push(stats.outcome);
        }
    }

    #[tokio::test]
    async fn abandoned_completed_fetch_reports_cancellation_or_timeout_once() {
        use serde_json::json;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/warehouses/wh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state":"RUNNING"})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id":"empty", "status":{"state":"SUCCEEDED"}, "manifest":{"chunks":[]}
            })))
            .mount(&server)
            .await;
        for timeout in [false, true] {
            let events = Arc::new(Events::default());
            let client = Arc::new(
                DbClient::new(&server.uri(), "wh", "token")
                    .with_protocol(Protocol::Sea)
                    .with_on_event(events.clone()),
            );
            let mut stream = pipeline::execute_lazy(client, "select 1 where false", None, None, None)
                .await
                .unwrap();
            stream.defer_completion = true;
            let mut stream = AgentResult(stream);
            stream.fetchmany_arrow(128).await.unwrap();
            assert!(stream.exhausted);
            assert!(events.0.lock().unwrap().is_empty());
            if timeout {
                stream.stats.store_outcome_if_unset(true);
            }
            drop(stream);
            assert_eq!(
                *events.0.lock().unwrap(),
                vec![if timeout { "timeout" } else { "cancelled" }]
            );
        }
    }

    #[test]
    fn invalid_budgets_and_unrepresentable_deadlines_are_rejected() {
        for options in [
            AgentOptions {
                max_rows: 0,
                ..AgentOptions::default()
            },
            AgentOptions {
                max_rows: usize::MAX,
                ..AgentOptions::default()
            },
            AgentOptions {
                max_bytes: 255,
                ..AgentOptions::default()
            },
            AgentOptions {
                max_bytes: usize::MAX,
                ..AgentOptions::default()
            },
            AgentOptions {
                max_cell_chars: 0,
                ..AgentOptions::default()
            },
            AgentOptions {
                max_cell_chars: usize::MAX,
                ..AgentOptions::default()
            },
            AgentOptions {
                total_timeout_s: f64::NAN,
                ..AgentOptions::default()
            },
            AgentOptions {
                total_timeout_s: f64::INFINITY,
                ..AgentOptions::default()
            },
            AgentOptions {
                total_timeout_s: f64::MAX,
                ..AgentOptions::default()
            },
            AgentOptions {
                total_timeout_s: -1.0,
                ..AgentOptions::default()
            },
        ] {
            assert!(options.validate().is_err());
        }
        assert!(AgentOptions::default().validate().is_ok());
    }
}
