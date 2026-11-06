require "test_helper"

# Documents how on_conflict: :discard behaves when it's used to keep one pending
# job per key. These pass: they record current behaviour, not a fix.
class SolidQueue::ConcurrencyDiscardDedupeTest < ActiveSupport::TestCase
  class DedupedJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard

    def perform(key) = nil
  end

  setup do
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "worker-123", metadata: { queue: "default" })
  end

  test "a call during a run is dropped, and the caller can't tell which job covers it" do
    running = DedupedJob.perform_later("during_run")
    SolidQueue::ReadyExecution.claim("default", 1, @process.id)

    call = DedupedJob.perform_later("during_run")

    assert_equal 1, SolidQueue::Job.where(class_name: DedupedJob.name).count
    assert_nil call.provider_job_id
    assert_not SolidQueue::Job.exists?(active_job_id: call.job_id)
    assert SolidQueue::Job.exists?(active_job_id: running.job_id)
  end

  test "delayed calls aren't checked until they're due, so each makes a scheduled job and one survives" do
    5.times { DedupedJob.set(wait: 5.minutes).perform_later("delayed") }
    assert_equal 5, SolidQueue::ScheduledExecution.count

    travel 6.minutes
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    assert_equal 1, SolidQueue::Job.where(class_name: DedupedJob.name).count
    assert_equal 1, SolidQueue::ReadyExecution.count
  end

  test "a dead worker's semaphore drops calls until its process is pruned" do
    DedupedJob.perform_later("dead_worker")
    SolidQueue::ReadyExecution.claim("default", 1, @process.id)

    travel SolidQueue.process_alive_threshold - 1.second
    dropped = DedupedJob.perform_later("dead_worker")
    assert_nil dropped.provider_job_id

    travel 2.seconds
    SolidQueue::Process.prune

    accepted = DedupedJob.perform_later("dead_worker")
    assert accepted.provider_job_id
  end
end
