# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceConfigChangeTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class ChangingJob < ApplicationJob
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 3

    def perform(*arguments)
      JobBuffer.add(arguments)

      if self.class.fail_next_run
        self.class.fail_next_run = false
        raise ExpectedTestError
      end
    end

    cattr_accessor :fail_next_run, default: false
  end

  class RetriedDiscardJob < ApplicationJob
    coalesce key: ->(name) { name }
    retry_on ExpectedTestError, wait: 1.hour

    def perform(name)
      raise ExpectedTestError
    end
  end

  setup { coalesce }

  teardown do
    coalesce
    ChangingJob.fail_next_run = false
  end

  test "a job created discarding is marked in its own payload, and its retries carry the marker" do
    RetriedDiscardJob.perform_later("a")
    RetriedDiscardJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_equal "discard", job.arguments["solid_queue_coalesce_on_conflict"]

    perform_ready_jobs

    retried = SolidQueue::Job.where(finished_at: nil).where.not(id: job.id).sole
    assert_equal "discard", retried.arguments["solid_queue_coalesce_on_conflict"]
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "the marker survives deserialization and serialization" do
    job = enqueue_calls

    payload = ActiveJob::Base.deserialize(job.arguments).serialize

    assert_equal "discard", payload["solid_queue_coalesce_on_conflict"]
  end

  test "only the job's own payload is marked, and instances and ordinary jobs aren't" do
    assert_equal "discard", enqueue_calls.arguments["solid_queue_coalesce_on_conflict"]

    assert_not AddToBufferJob.new("plain").serialize.key?("solid_queue_coalesce_on_conflict")
    assert_not ChangingJob.new(1).serialize.key?("solid_queue_coalesce_on_conflict")
  end

  test "enqueueing the same instance twice doesn't mark it" do
    active_job = ChangingJob.new(1)
    active_job.enqueue
    active_job.enqueue

    assert_equal :discarded, active_job.coalesce_status
    assert_not active_job.serialize.key?("solid_queue_coalesce_on_conflict")
    assert_equal "discard", SolidQueue::Job.sole.arguments["solid_queue_coalesce_on_conflict"]
  end

  test "a job whose class stopped coalescing runs with its own arguments and gives up its key" do
    job = enqueue_calls
    stop_coalescing(ChangingJob)

    assert_nil ChangingJob.perform_later(3).coalesce_status
    perform_ready_jobs

    assert_equal [ [ 1 ], [ 3 ] ], JobBuffer.values
    assert job.reload.finished?
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a job whose class stopped coalescing still gives up its key when it fails, and runs when it's retried" do
    job = enqueue_calls
    stop_coalescing(ChangingJob)
    SolidQueue::ReadyExecution.delete_all

    job.failed_with(ExpectedTestError.new("boom"))
    assert_equal 0, SolidQueue::CoalesceKey.count

    coalesce
    assert_equal :created, ChangingJob.perform_later(3).coalesce_status

    job.retry
    perform_ready_jobs

    assert_equal [ [ 1 ], [ 3 ] ], JobBuffer.values.sort
  end

  test "a job whose class was removed fails and gives up its key, and runs when the class is back and it's retried" do
    job = enqueue_calls
    rename_job_class(job, "RemovedChangingJob")

    assert_raises(NameError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, ChangingJob.perform_later(3).coalesce_status

    rename_job_class(job, ChangingJob.name)
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ 1 ], [ 3 ] ], JobBuffer.values.sort
  end

  test "a job waiting under a key that changed still runs, and new calls open a job under the new key" do
    job = enqueue_calls
    ChangingJob.coalesce key: "another"

    second = ChangingJob.perform_later(3)
    assert_equal :created, second.coalesce_status
    assert_equal :discarded, ChangingJob.perform_later(4).coalesce_status
    assert_equal 2, SolidQueue::CoalesceKey.count
    assert_coalesce_invariants

    perform_ready_jobs

    assert_equal [ [ 1 ], [ 3 ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "the key and the job stay consistent through a job's life" do
    job = enqueue_calls
    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants

    ChangingJob.fail_next_run = true
    perform_ready_jobs
    assert_equal 0, SolidQueue::CoalesceKey.count, "the open job released its key when it started"

    retrying_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal job.active_job_id, retrying_job.active_job_id
    assert_coalesce_invariants

    open_job = ChangingJob.perform_later(3).provider_job_id
    assert_equal open_job, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants

    SolidQueue::Job.find(open_job).discard
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_coalesce_invariants
  end

  test "a rolled back call leaves the open job and its key as they were" do
    job = enqueue_calls

    SolidQueue::Job.transaction do
      ChangingJob.perform_later(3)
      raise ActiveRecord::Rollback
    end

    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal job.arguments, job.reload.arguments
    assert_coalesce_invariants
  end

  test "a job that finishes, preserved or not, leaves no key" do
    preserved = enqueue_calls
    perform_ready_jobs
    assert preserved.reload.finished?
    assert_equal 0, SolidQueue::CoalesceKey.count

    SolidQueue.with(preserve_finished_jobs: false) do
      ChangingJob.perform_later(3)
      perform_ready_jobs
    end

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_coalesce_invariants
  end

  test "a job that fails with an open key, then is discarded, leaves nothing behind" do
    job = enqueue_calls
    job.failed_with(ExpectedTestError.new("boom"))
    assert_equal 0, SolidQueue::CoalesceKey.count

    job.failed_execution.discard

    assert_not SolidQueue::Job.exists?(job.id)
    assert_coalesce_invariants
  end

  test "deleting jobs in bulk or directly leaves no key behind" do
    bulk_job = enqueue_calls
    SolidQueue::ReadyExecution.discard_all_from_jobs([ bulk_job ])
    assert_equal 0, SolidQueue::CoalesceKey.count

    direct_job = enqueue_calls
    SolidQueue::Job.where(id: direct_job.id).delete_all
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  private
    def coalesce
      ChangingJob.coalesce key: "changing"
    end

    def enqueue_calls
      ChangingJob.perform_later(1)
      ChangingJob.perform_later(2)
      SolidQueue::Job.sole
    end
end
