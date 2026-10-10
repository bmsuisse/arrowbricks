//! Per-call context that follows a statement into every task it spawns.
//!
//! A `TokenProvider` (or `EventSink`) backed by a caller's own runtime may
//! need to know *which call* it is serving -- `lib.rs`'s `PyTokenProvider`
//! needs the asyncio event loop of the Python coroutine that started the
//! statement, so it can run an async `token_provider` on that loop. The
//! caller sets that context once, at the entry point (`with_call_context`),
//! where it is known. Every task this crate spawns for the call is wrapped
//! in `in_call_context`, so the same context is still there when a chunk
//! worker, the Thrift fetch loop, a heartbeat-wrapped submit or a
//! best-effort cancel asks for a token. Tokio task-locals are not inherited
//! by spawned tasks on their own; without the wrap, a token request from
//! one of those tasks had no way to find the caller's loop (the bug fixed in 5.2.1:
//! `stream_query_json` on a cold client failed with "no running event
//! loop").
//!
//! The context is opaque here (`Arc<dyn Any>`) so this module stays free of
//! PyO3, like the rest of `client`; `lib.rs` stores a `TaskLocals` in it and
//! downcasts it back.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

/// Opaque per-call state, set by the caller at the entry point. See the
/// module doc comment.
pub type CallContext = Arc<dyn Any + Send + Sync>;

tokio::task_local! {
    static CALL_CONTEXT: Option<CallContext>;
}

/// The context of the call the current task belongs to, if any.
pub fn current_call_context() -> Option<CallContext> {
    CALL_CONTEXT.try_with(Clone::clone).ok().flatten()
}

/// Runs `fut` with `ctx` as its call context. For entry points: wrap the
/// whole future of one public call in this.
pub fn with_call_context<F: Future>(ctx: Option<CallContext>, fut: F) -> impl Future<Output = F::Output> {
    CALL_CONTEXT.scope(ctx, fut)
}

/// Runs the synchronous `f` with `ctx` as the call context -- for an entry
/// point that builds (and spawns) its work synchronously, such as
/// `ResultSet.fetchall_arrow_streamed`, so the tasks it spawns and the
/// cancel hook it builds pick `ctx` up.
pub fn enter_call_context<R>(ctx: Option<CallContext>, f: impl FnOnce() -> R) -> R {
    CALL_CONTEXT.sync_scope(ctx, f)
}

/// Wraps a future that is about to be spawned so it keeps the call context
/// of the code spawning it. Captured now, not when the task first runs.
/// Every `spawn` in this crate that can reach the token provider (directly
/// or through a task it spawns in turn) goes through this.
pub fn in_call_context<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    with_call_context(current_call_context(), fut)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_value(ctx: Option<CallContext>) -> Option<u32> {
        ctx.and_then(|c| c.downcast_ref::<u32>().copied())
    }

    #[tokio::test]
    async fn spawned_tasks_inherit_the_call_context_only_through_in_call_context() {
        let ctx: CallContext = Arc::new(7u32);
        let (wrapped, bare, nested) = with_call_context(Some(ctx), async {
            let wrapped = tokio::spawn(in_call_context(async { ctx_value(current_call_context()) }));
            let bare = tokio::spawn(async { ctx_value(current_call_context()) });
            // Two levels deep, like the SEA fetch driver and its workers.
            let nested = tokio::spawn(in_call_context(async {
                tokio::spawn(in_call_context(async { ctx_value(current_call_context()) }))
                    .await
                    .unwrap()
            }));
            (wrapped.await.unwrap(), bare.await.unwrap(), nested.await.unwrap())
        })
        .await;
        assert_eq!(wrapped, Some(7));
        assert_eq!(bare, None, "a plain tokio::spawn does not inherit task-locals");
        assert_eq!(nested, Some(7));
        assert_eq!(ctx_value(current_call_context()), None);
    }

    #[test]
    fn enter_call_context_is_seen_by_futures_wrapped_inside_it() {
        let ctx: CallContext = Arc::new(3u32);
        let fut = enter_call_context(Some(ctx), || {
            in_call_context(async { ctx_value(current_call_context()) })
        });
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        assert_eq!(rt.block_on(fut), Some(3));
    }

    #[tokio::test]
    async fn the_innermost_scope_wins() {
        let outer: CallContext = Arc::new(1u32);
        let inner: CallContext = Arc::new(2u32);
        let seen = with_call_context(Some(outer), async move {
            with_call_context(Some(inner), async { ctx_value(current_call_context()) }).await
        })
        .await;
        assert_eq!(seen, Some(2));
    }
}
