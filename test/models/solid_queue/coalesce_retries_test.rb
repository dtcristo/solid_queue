# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceRetriesTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class FlakyJob < ApplicationJob
    coalesce key: ->(group, *) { group }
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 3

    def perform(*arguments)
      JobBuffer.add([ executions, arguments ])
      raise ExpectedTestError if executions < self.class.succeed_on
    end

    cattr_accessor :succeed_on, default: 2
  end

  class ManuallyRetriedJob < ApplicationJob
    coalesce key: "manual"
    rescue_from(ExpectedTestError) { retry_job }

    def perform(*arguments)
      JobBuffer.add([ executions, arguments ])
      raise ExpectedTestError if executions == 1
    end
  end

  class FailingJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*arguments)
      JobBuffer.add(arguments)
      raise ExpectedTestError unless self.class.fixed
    end

    cattr_accessor :fixed, default: false
  end

  setup do
    FlakyJob.succeed_on = 2
    FailingJob.fixed = false
  end

  test "retry_on runs the job again with its own arguments, and later calls don't join the retry" do
    FlakyJob.perform_later("g", 1)
    FlakyJob.perform_later("g", 2)
    old_job = SolidQueue::Job.sole

    # A call that arrives during the failed run opens a new job
    during = nil
    perform_ready_jobs_with_callback { during ||= FlakyJob.perform_later("g", 3) }

    assert old_job.reload.finished?
    retry_job = SolidQueue::Job.where(active_job_id: old_job.active_job_id, finished_at: nil).sole
    assert_not SolidQueue::CoalesceKey.exists?(job_id: retry_job.id)
    assert_equal :created, during.coalesce_status
    assert_equal during.provider_job_id, SolidQueue::CoalesceKey.sole.job_id

    perform_ready_jobs

    assert_equal [ [ 1, [ "g", 1 ] ], [ 2, [ "g", 1 ] ], [ 1, [ "g", 3 ] ] ], JobBuffer.values.sort_by { |executions, arguments| [ arguments.last, executions ] }
  end

  test "rescue_from with retry_job runs the job again" do
    ManuallyRetriedJob.perform_later("a")
    ManuallyRetriedJob.perform_later("b")

    perform_ready_jobs
    perform_ready_jobs

    assert_equal [ [ 1, [ "a" ] ], [ 2, [ "a" ] ] ], JobBuffer.values
  end

  test "exhausted retries leave a failed execution, and a manual retry runs it without taking a key" do
    FlakyJob.succeed_on = 100
    FlakyJob.perform_later("g", 1)

    perform_ready_jobs
    perform_ready_jobs
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    failed = SolidQueue::FailedExecution.sole.job
    assert_equal 0, SolidQueue::CoalesceKey.count

    FlakyJob.succeed_on = 1
    failed.retry
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, FlakyJob.perform_later("g", 2).coalesce_status

    JobBuffer.clear
    perform_ready_jobs

    assert_equal [ [ 1, [ "g", 1 ] ], [ 1, [ "g", 2 ] ] ], JobBuffer.values.sort_by(&:last)
    assert failed.reload.finished?
  end

  test "a job released before it started stays open until it starts" do
    FailingJob.fixed = true
    FailingJob.perform_later("g", 1)
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashed-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id).sole.release

    assert_equal :discarded, FailingJob.perform_later("g", 2).coalesce_status
    perform_ready_jobs

    assert_equal [ [ "g", 1 ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "a job of a pruned process that had started fails, and the key it gave up stays free" do
    FailingJob.perform_later("g", 1)
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "pruned-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id)
    job.start_coalesced_run
    assert_equal 0, SolidQueue::CoalesceKey.count

    process.update_column(:last_heartbeat_at, 1.day.ago)
    SolidQueue::Process.prune

    assert job.reload.failed?
    assert_equal :created, FailingJob.perform_later("g", 2).coalesce_status
  end

  test "a failed job releases the key it held" do
    FailingJob.perform_later("g", 1)
    job = SolidQueue::Job.sole
    SolidQueue::ReadyExecution.delete_all

    job.failed_with(ExpectedTestError.new("boom"))

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, FailingJob.perform_later("g", 2).coalesce_status
  end

  test "a failed job that's retried runs without joining or taking a key" do
    FailingJob.perform_later("g", 1)
    assert_raises(ExpectedTestError) { perform_ready_jobs }
    job = SolidQueue::FailedExecution.sole.job
    open_job = FailingJob.perform_later("g", 2).provider_job_id

    FailingJob.fixed = true
    job.retry

    assert_equal open_job, SolidQueue::CoalesceKey.sole.job_id
    perform_ready_jobs
    assert_equal [ [ "g", 1 ], [ "g", 1 ], [ "g", 2 ] ], JobBuffer.values.sort_by(&:last)
  end

  # The gap the marker leaves open, kept on purpose. Nothing says this job can hold a
  # key, so finishing or failing it doesn't look for one. Only jobs from before the
  # marker can get here.
  test "an unmarked job whose class stopped coalescing keeps its key after finishing, until it's cleared" do
    job = enqueue_unmarked_job_of_another_class

    SolidQueue.with(preserve_finished_jobs: true) { perform_ready_jobs }

    assert job.reload.finished?
    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id

    SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, FailingJob.perform_later("g", 2).coalesce_status
  end

  test "a call can be discarded into an unmarked finished job that kept its key, and that call won't run" do
    job = enqueue_unmarked_job_of_another_class
    SolidQueue.with(preserve_finished_jobs: true) { perform_ready_jobs }

    joined = FailingJob.perform_later("g", 2)

    assert_equal :discarded, joined.coalesce_status
    assert_equal job.id, joined.provider_job_id
    assert_empty perform_ready_jobs
  end

  test "an unmarked job whose class stopped coalescing fails without looking for its key, and discarding it releases the key" do
    job = enqueue_unmarked_job_of_another_class
    SolidQueue::ReadyExecution.delete_all

    job.failed_with(ExpectedTestError.new("boom"))

    assert job.reload.failed?
    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id

    job.discard

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, FailingJob.perform_later("g", 2).coalesce_status
  end

  private
    def enqueue_unmarked_job_of_another_class
      FailingJob.perform_later("g", 1)

      SolidQueue::Job.sole.tap do |job|
        rename_job_class(job, "AddToBufferJob")
        job.update!(arguments: AddToBufferJob.new("no longer coalescing").serialize)
      end
    end
end
