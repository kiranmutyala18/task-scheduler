# Notes

- A heap keeps the next deadline at the front in O(log n) insertion time.
- `time.monotonic()` avoids wrong delays when the wall clock changes.
- One dispatcher owns timer ordering; a worker pool runs slow tasks without blocking scheduling.
- Cancellation is cooperative: a pending task is marked cancelled, while a running Python function cannot be safely killed from another thread.
- Shutdown cancels future delayed tasks and lets queued/running work finish before workers exit.
- V2 adds recurring work and scheduler metrics. Recurring tasks schedule the next execution only after the current execution finishes, which prevents overlap for the same recurring job.
