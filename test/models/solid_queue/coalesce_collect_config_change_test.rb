# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceCollectConfigChangeTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class ChangingJob < ApplicationJob
    retry_on StandardError, wait: 0.seconds, attempts: 3
    discard_on ActiveJob::Coalescing::CollectedJobMismatch

    def perform(*arguments)
      JobBuffer.add([ :perform, arguments ])
    end

    def perform_calls(calls)
      JobBuffer.add([ :perform_calls, calls.map(&:arguments) ])

      if self.class.fail_next_run
        self.class.fail_next_run = false
        raise ExpectedTestError
      end
    end

    cattr_accessor :fail_next_run, default: false
  end

  class NoPerformCallsJob < ApplicationJob
    coalesce key: "no-perform-calls", on_conflict: :collect
  end

  class LimitedJob < ApplicationJob
    coalesce key: "limited", on_conflict: :collect
    limits_concurrency key: "limited", to: 1, on_conflict: :block

    def perform_calls(calls)
      JobBuffer.add([ :perform_calls, calls.map(&:arguments) ])
    end
  end

  setup { collect }

  teardown do
    collect
    ChangingJob.fail_next_run = false
    LimitedJob.limits_concurrency key: "limited", to: 1, on_conflict: :block
    LimitedJob.coalesce key: "limited", on_conflict: :collect
  end

  test "a collecting job whose class stopped coalescing fails and keeps its calls" do
    job = enqueue_calls
    stop_coalescing(ChangingJob)

    error = assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_match ChangingJob.name, error.message
    assert_match(/retry|discard/, error.message)
    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
  end

  test "a collecting job whose class switched to discarding fails and keeps its calls" do
    job = enqueue_calls
    discard

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
  end

  test "a job failed for a changed class runs all of its calls when it's restored and retried" do
    job = enqueue_calls
    stop_coalescing(ChangingJob)
    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) do
      SolidQueue::Job.find(job.id).retry
      perform_ready_jobs
    end
    assert job.reload.failed?
    assert_empty JobBuffer.values

    collect
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ] ] ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "a job waiting to retry when its class changes fails, and a restored retry still has its calls" do
    ChangingJob.fail_next_run = true
    enqueue_calls
    perform_ready_jobs
    retrying_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal 2, retrying_job.coalesced_calls.count
    stop_coalescing(ChangingJob)

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }
    assert retrying_job.reload.failed?

    collect
    retrying_job.retry
    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ] ] ] ] * 2, JobBuffer.values
  end

  test "a collecting job whose class lost perform_calls fails and keeps its calls until it's restored" do
    job = enqueue_calls
    rename_job_class(job, NoPerformCallsJob.name)

    assert_raises(NotImplementedError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count

    rename_job_class(job, ChangingJob.name)
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ] ] ] ], JobBuffer.values
  end

  test "a concurrency conflict doesn't destroy a collecting job whose class now discards conflicts" do
    LimitedJob.set(wait: 1.hour).perform_later(1)
    LimitedJob.set(wait: 1.hour).perform_later(2)
    job = SolidQueue::Job.sole
    stop_limited_job_collecting
    LimitedJob.perform_later(3)

    travel_to(2.hours.from_now) { SolidQueue::ScheduledExecution.dispatch_next_batch(10) }

    assert job.reload.blocked?
    assert_equal 2, job.coalesced_calls.count
  end

  test "retrying a failed collecting job through a conflict doesn't destroy it either" do
    LimitedJob.perform_later(1)
    LimitedJob.perform_later(2)
    job = SolidQueue::Job.sole
    stop_limited_job_collecting
    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }
    LimitedJob.perform_later(3)

    SolidQueue::Job.find(job.id).retry

    assert job.reload.blocked?
    assert_equal 2, job.coalesced_calls.count
  end

  test "a collecting job drained after its class changed adapter still runs all of its calls" do
    job = enqueue_calls
    ChangingJob.queue_adapter = :test

    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ] ] ] ], JobBuffer.values
    assert job.reload.finished?
  ensure
    ChangingJob.queue_adapter = :solid_queue
  end

  test "a retry on another adapter fails the job with its calls, and restoring the adapter runs them" do
    job = enqueue_calls
    ChangingJob.fail_next_run = true
    ChangingJob.queue_adapter = :test

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count

    ChangingJob.queue_adapter = :solid_queue
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ] ] ] ] * 2, JobBuffer.values
    assert job.reload.finished?
  ensure
    ChangingJob.queue_adapter = :solid_queue
  end

  test "a collecting job that lost its claim doesn't run its own arguments" do
    job = enqueue_calls
    active_job = ActiveJob::Base.deserialize(job.arguments.merge("provider_job_id" => job.id))

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { active_job.perform_now }

    assert_empty JobBuffer.values
    assert_equal 2, job.coalesced_calls.count
  end

  test "enqueueing the same instance twice doesn't mark its call" do
    active_job = ChangingJob.new(1)
    active_job.enqueue
    active_job.enqueue

    assert_not SolidQueue::CoalescedCall.pluck(:arguments).any? { |payload| payload.key?("solid_queue_coalesce_on_conflict") }
    assert_equal "collect", SolidQueue::Job.sole.arguments["solid_queue_coalesce_on_conflict"]
  end

  test "the collect marker survives deserialization and serialization" do
    job = enqueue_calls

    payload = ActiveJob::Base.deserialize(job.arguments).serialize

    assert_equal "collect", payload["solid_queue_coalesce_on_conflict"]
  end

  test "a collecting job that lost all of its calls fails instead of running its own arguments" do
    job = enqueue_calls
    SolidQueue::CoalescedCall.where(job_id: job.id).delete_all

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
  end

  test "a collecting job that lost some of its calls runs the rest" do
    job = enqueue_calls
    job.coalesced_calls.order(:id).first.delete

    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 2 ] ] ] ], JobBuffer.values
  end

  test "a call to a class switched to collecting doesn't join a discarding job, and both calls run" do
    discard
    first = ChangingJob.perform_later(1)
    collect

    second = ChangingJob.perform_later(2)

    assert_equal :created, second.coalesce_status
    assert_not_equal first.provider_job_id, second.provider_job_id
    assert_coalesce_invariants

    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ] ] ], [ :perform_calls, [ [ 2 ] ] ] ], JobBuffer.values
  end

  test "a call to a class switched to discarding isn't suppressed by a collecting job" do
    enqueue_calls
    discard

    call = ChangingJob.perform_later(3)

    assert_equal :created, call.coalesce_status
    assert_equal call.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants
    assert_equal :discarded, ChangingJob.perform_later(4).coalesce_status
  end

  test "max_calls or a key changed while calls wait doesn't touch them" do
    job = enqueue_calls
    ChangingJob.coalesce key: "changing", on_conflict: :collect, max_calls: 50
    ChangingJob.perform_later(3)
    ChangingJob.coalesce key: "another", on_conflict: :collect, max_calls: 1
    ChangingJob.perform_later(4)

    assert_equal 3, job.coalesced_calls.count
    assert_coalesce_invariants

    perform_ready_jobs

    assert_equal [ [ :perform_calls, [ [ 1 ], [ 2 ], [ 3 ] ] ], [ :perform_calls, [ [ 4 ] ] ] ], JobBuffer.values
  end

  test "only the job's own payload is marked, not its calls, and ordinary jobs aren't" do
    job = enqueue_calls

    assert_equal "collect", job.arguments["solid_queue_coalesce_on_conflict"]
    assert_not job.coalesced_calls.any? { |call| call.arguments.key?("solid_queue_coalesce_on_conflict") }

    assert_not AddToBufferJob.new("plain").serialize.key?("solid_queue_coalesce_on_conflict")
    assert_not ChangingJob.new(1).serialize.key?("solid_queue_coalesce_on_conflict")
  end

  test "the key, the job and the calls stay consistent through a job's life" do
    job = enqueue_calls
    assert_coalesce_invariants

    ChangingJob.perform_later(3)
    assert_equal 0, SolidQueue::CoalesceKey.count, "a full job is sealed"
    assert_coalesce_invariants

    open_job = ChangingJob.perform_later(4).provider_job_id
    assert_equal open_job, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants

    ChangingJob.fail_next_run = true
    perform_ready_jobs
    assert_equal 0, SolidQueue::CoalesceKey.count, "the open job released its key when it started"

    retrying_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal 3, retrying_job.coalesced_calls.count
    assert_equal job.active_job_id, retrying_job.active_job_id
    assert_equal 0, job.reload.coalesced_calls.count, "the retry took the calls with it"
    assert_coalesce_invariants

    retrying_job.discard
    assert_equal 0, SolidQueue::CoalescedCall.where(job_id: retrying_job.id).count
    assert_coalesce_invariants
  end

  test "a rolled back call leaves the open job's count and calls as they were" do
    job = enqueue_calls

    SolidQueue::Job.transaction do
      ChangingJob.perform_later(3)
      raise ActiveRecord::Rollback
    end

    assert_equal 2, job.coalesced_calls.count
    assert_equal 2, SolidQueue::CoalesceKey.sole.calls_count
    assert_coalesce_invariants
  end

  test "a job that finishes, preserved or not, leaves its key and calls consistent" do
    preserved = enqueue_calls
    perform_ready_jobs
    assert preserved.reload.finished?
    assert_equal 2, preserved.coalesced_calls.count
    assert_coalesce_invariants

    SolidQueue.with(preserve_finished_jobs: false) do
      ChangingJob.perform_later(3)
      perform_ready_jobs
    end

    assert_equal 2, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_coalesce_invariants
  end

  test "a job that fails with an open key, then is discarded, leaves nothing behind" do
    job = enqueue_calls
    job.failed_with(ExpectedTestError.new("boom"))
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 2, job.coalesced_calls.count

    job.failed_execution.discard

    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_coalesce_invariants
  end

  test "deleting jobs in bulk or directly leaves no key or call behind" do
    bulk_job = enqueue_calls
    SolidQueue::ReadyExecution.discard_all_from_jobs([ bulk_job ])
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count

    direct_job = enqueue_calls
    SolidQueue::Job.where(id: direct_job.id).delete_all
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  private
    def collect
      ChangingJob.coalesce key: "changing", on_conflict: :collect, max_calls: 3
    end

    def discard
      ChangingJob.coalesce key: "changing", on_conflict: :discard
    end

    def stop_limited_job_collecting
      stop_coalescing(LimitedJob)
      LimitedJob.limits_concurrency key: "limited", to: 1, on_conflict: :discard
    end

    def enqueue_calls
      ChangingJob.perform_later(1)
      ChangingJob.perform_later(2)
      SolidQueue::Job.sole
    end
end
