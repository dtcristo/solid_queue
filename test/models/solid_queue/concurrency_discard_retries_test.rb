require "test_helper"

# A retry is the same work continuing. With on_conflict: :discard it must not
# disappear: it should either wait for the semaphore or leave a failed execution.
class SolidQueue::ConcurrencyDiscardRetriesTest < ActiveSupport::TestCase
  # One group, so a job of another class can hold the semaphore
  GROUP = "ConcurrencyDiscardRetries"

  class ImmediateRetryJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP
    retry_on RuntimeError, wait: 0.seconds, attempts: 3

    def perform(key)
      JobBuffer.add("#{key}: attempt #{executions}")
      raise "boom"
    end
  end

  class DelayedRetryJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP
    retry_on RuntimeError, wait: 1.minute, attempts: 3

    def perform(key)
      JobBuffer.add("#{key}: attempt #{executions}")
      raise "boom"
    end
  end

  class RetryJobFromRescueJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP
    rescue_from(RuntimeError) { retry_job }

    def perform(key)
      JobBuffer.add("#{key}: attempt #{executions}")
      raise "boom" if executions == 1
    end
  end

  class FailingJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP

    def perform(key)
      JobBuffer.add("#{key}: attempt #{executions}")
      raise "boom" if executions == 1
    end
  end

  class HoldingJob < ApplicationJob
    limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP

    def perform(key) = nil
  end

  setup do
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "worker-123", metadata: { queue: "default" })
    JobBuffer.clear
  end

  test "an immediate retry_on retry survives its own running job's semaphore" do
    active_job = ImmediateRetryJob.perform_later("immediate")

    perform_claimed(active_job)

    assert_retry_kept active_job
  end

  test "a retry_job from rescue_from survives its own running job's semaphore" do
    active_job = RetryJobFromRescueJob.perform_later("rescue_from")

    perform_claimed(active_job)

    assert_retry_kept active_job
  end

  test "a delayed retry_on retry survives another job holding the semaphore when it's due" do
    active_job = DelayedRetryJob.perform_later("delayed")
    perform_claimed(active_job)
    assert SolidQueue::ScheduledExecution.exists?, "the retry should be scheduled"

    holding_semaphore("delayed") do
      travel 2.minutes
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    end

    assert_retry_kept active_job
  end

  test "FailedExecution#retry keeps the failed job when another job holds the semaphore" do
    active_job = FailingJob.perform_later("manual")
    perform_claimed(active_job)
    job = SolidQueue::Job.find_by!(active_job_id: active_job.job_id)
    assert job.failed?

    holding_semaphore("manual") { job.failed_execution.retry }

    assert SolidQueue::Job.exists?(job.id), "the failed job was deleted, with no failed execution left"
  end

  test "FailedExecution.retry_all keeps the failed job when another job holds the semaphore" do
    active_job = FailingJob.perform_later("retry_all")
    perform_claimed(active_job)
    job = SolidQueue::Job.find_by!(active_job_id: active_job.job_id)
    assert job.failed?

    holding_semaphore("retry_all") { SolidQueue::FailedExecution.retry_all(SolidQueue::Job.where(id: job.id)) }

    assert SolidQueue::Job.exists?(job.id), "the failed job was deleted, with no failed execution left"
  end

  if defined?(ActiveJob::Continuable)
    class InterruptedJob < ApplicationJob
      include ActiveJob::Continuable

      limits_concurrency key: ->(key) { key }, to: 1, on_conflict: :discard, group: GROUP

      def perform(key)
        step(:one) { JobBuffer.add("#{key}: step one") }
        step(:two) { JobBuffer.add("#{key}: step two") }
      end
    end

    test "a continuation's resume survives another job holding the semaphore when it's due" do
      active_job = InterruptedJob.perform_later("continuation")

      ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = true
      perform_claimed(active_job)
      ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = false
      assert SolidQueue::ScheduledExecution.exists?, "the resume should be scheduled"

      holding_semaphore("continuation") do
        travel 1.minute
        SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      end

      assert_retry_kept active_job
    ensure
      ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = false
    end
  end

  private
    def perform_claimed(active_job)
      job = SolidQueue::Job.find_by!(active_job_id: active_job.job_id)
      assert job.ready?, "#{active_job.class.name} should be ready"

      SolidQueue::ReadyExecution.claim(job.queue_name, 1, @process.id)
      SolidQueue::ClaimedExecution.find_by!(job_id: job.id).perform
    rescue RuntimeError
      # A job that fails raises out of perform, after recording its failure
    end

    # Another job with the same key is running, so it holds the semaphore
    def holding_semaphore(key)
      holder = SolidQueue::Job.find_by!(active_job_id: HoldingJob.perform_later(key).job_id)
      assert holder.ready?, "the holding job should get the semaphore"
      SolidQueue::ReadyExecution.claim(holder.queue_name, 1, @process.id)

      yield
    end

    def assert_retry_kept(active_job)
      jobs = SolidQueue::Job.where(active_job_id: active_job.job_id)
      pending = jobs.where(finished_at: nil).where.missing(:failed_execution)
      failed = jobs.joins(:failed_execution)

      assert pending.exists? || failed.exists?, "the retry was discarded and no failure was recorded. Attempts that ran: #{JobBuffer.values.inspect}"
    end
end
