# frozen_string_literal: true

require "test_helper"

# Coalesced jobs as members of a SolidQueue::Batch. Solid Queue's accounting stays
# authoritative, so these look at the batch's jobs, counters and callbacks.
class SolidQueue::CoalesceCollectBatchTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Enqueues inside the batch's transaction, whatever the app's defaults say
  class ImmediateJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
  end

  class OrdinaryJob < ImmediateJob
    def perform(*); end
  end

  class DiscardJob < ImmediateJob
    coalesce key: ->(group, *) { group }

    def perform(*arguments)
      JobBuffer.add(arguments)
    end
  end

  class CollectJob < ImmediateJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class RetriedJob < ApplicationJob
    coalesce key: ->(group) { group }
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 2

    def perform(group)
      raise ExpectedTestError if executions == 1

      JobBuffer.add(group)
    end
  end

  class RetriedCollectJob < ApplicationJob
    coalesce key: ->(group) { group }, on_conflict: :collect
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 2

    def perform_calls(calls)
      raise ExpectedTestError if executions == 1

      JobBuffer.add(calls.size)
    end
  end

  class ReadsBatchJob < ApplicationJob
    coalesce key: ->(group) { group }

    def perform(*)
      JobBuffer.add(batch.id)
    end
  end

  class ReadsBatchCollectJob < ApplicationJob
    coalesce key: ->(group) { group }, on_conflict: :collect

    def perform_calls(_calls)
      JobBuffer.add(batch.id)
    end
  end

  class FailingAdmissionJob < ImmediateJob
    coalesce key: ->(group) { group }

    def concurrency_key
      raise ExpectedTestError
    end

    def perform(*); end
  end

  # Sorts after the other classes in a bulk enqueue, so it's admitted last
  class TrailingDeadlockJob < ImmediateJob
    coalesce key: ->(group) { group }

    def concurrency_key
      JobBuffer.add(:admission)
      raise ActiveRecord::Deadlocked, "boom"
    end

    def perform(*); end
  end

  class CallbackJob < ApplicationJob
    def perform
      JobBuffer.add(:callback)
    end
  end

  class CoalescedCallbackJob < ApplicationJob
    coalesce key: "callback"

    def perform; end
  end

  RACED_RUNS = Concurrent::Array.new

  setup do
    RACED_RUNS.clear
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "batch-worker")
  end

  teardown do
    SolidQueue::Job.destroy_all
    SolidQueue::Batch.destroy_all
  end

  test "calls with the same key in a batch share one job" do
    calls = []
    events = []
    batch = nil
    callback = ->(*, payload) { events << payload }
    ActiveSupport::Notifications.subscribed(callback, "coalesce.solid_queue") do
      batch = SolidQueue::Batch.enqueue { 3.times { |i| calls << DiscardJob.perform_later("a", i) } }
    end

    assert_equal [ "Batch/#{batch.id}/SolidQueue::CoalesceCollectBatchTest::DiscardJob/a" ] * 3, events.map { |payload| payload[:key] }
    assert_equal [ :created, :discarded, :discarded ], calls.map(&:coalesce_status)
    assert_equal [ batch.jobs.sole.id ], calls.map(&:provider_job_id).uniq
    assert_equal 1, batch.reload.total_jobs

    perform_ready_jobs

    assert_equal [ [ "a", 0 ] ], JobBuffer.values
    assert batch.reload.finished?
    assert_equal 1, batch.completed_jobs
  end

  test "a job can read its batch in perform and perform_calls" do
    batch = SolidQueue::Batch.enqueue do
      ReadsBatchJob.perform_later("a")
      ReadsBatchCollectJob.perform_later("a")
    end

    perform_ready_jobs

    assert_equal [ batch.id, batch.id ], JobBuffer.values
  end

  test "a collecting job belongs to its batch, and so does the job after it seals" do
    batch = SolidQueue::Batch.enqueue { 3.times { |i| CollectJob.perform_later("a", i) } }

    assert_equal 2, batch.reload.total_jobs
    assert_equal 2, batch.jobs.count

    perform_ready_jobs

    assert_equal [ [ [ "a", 0 ], [ "a", 1 ] ], [ [ "a", 2 ] ] ], JobBuffer.values
    assert batch.reload.finished?
  end

  test "each batch collects its own calls" do
    first = SolidQueue::Batch.enqueue { CollectJob.perform_later("a", 1) }
    second = SolidQueue::Batch.enqueue { CollectJob.perform_later("a", 2) }

    perform_ready_jobs

    assert_equal [ [ [ "a", 1 ] ], [ [ "a", 2 ] ] ], JobBuffer.values.sort
    assert first.reload.finished?
    assert second.reload.finished?
  end

  test "a coalesced callback job stays outside the batch" do
    SolidQueue::Batch.enqueue(on_success: CoalescedCallbackJob) { DiscardJob.perform_later("a") }

    perform_ready_jobs

    callback = SolidQueue::Job.find_by!(finished_at: nil)
    assert_nil callback.batch_id
    assert_equal callback.id, CoalescedCallbackJob.perform_later.provider_job_id
  end

  test "perform_all_later in a batch coalesces and mixes with ordinary jobs" do
    calls = [ DiscardJob.new("a", 1), DiscardJob.new("a", 2), CollectJob.new("c", 3), CollectJob.new("c", 4), CollectJob.new("c", 5), OrdinaryJob.new("x") ]

    batch = SolidQueue::Batch.enqueue { ActiveJob.perform_all_later(calls) }

    assert_equal [ :created, :discarded, :created, :collected, :created, nil ], calls.map(&:coalesce_status)
    assert_equal 4, batch.reload.total_jobs
    assert_equal 4, batch.jobs.count
    assert_not batch.finished?

    perform_ready_jobs

    assert batch.reload.finished?
    assert_equal 4, batch.completed_jobs
  end

  test "enqueuing into a finished batch raises and holds no key" do
    single, bulk, collect = nil
    batch = SolidQueue::Batch.enqueue do
      single = DiscardJob.new("a")
      bulk = DiscardJob.new("b")
      collect = CollectJob.new("c")
    end
    assert batch.reload.finished?

    assert_raises(SolidQueue::Batch::AlreadyFinished) { single.enqueue }
    assert_raises(SolidQueue::Batch::AlreadyFinished) { ActiveJob.perform_all_later(bulk) }
    assert_raises(SolidQueue::Batch::AlreadyFinished) { collect.enqueue }

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "a block that raises rolls back the batch and everything in it" do
    assert_raises(ExpectedTestError) do
      SolidQueue::Batch.enqueue(on_success: CallbackJob) do
        DiscardJob.perform_later("a")
        CollectJob.perform_later("c")
        raise ExpectedTestError
      end
    end

    assert_nothing_stored
    assert_equal 0, SolidQueue::Batch.count
  end

  test "a failure partway through a mixed bulk enqueue leaves the batch as it was" do
    batch = held_open_batch

    assert_raises(ExpectedTestError) do
      batch.enqueue { ActiveJob.perform_all_later(OrdinaryJob.new("x"), CollectJob.new("c"), DiscardJob.new("a"), FailingAdmissionJob.new("z")) }
    end

    assert_batch_only_holds_its_first_job(batch)
  end

  test "a deadlock inside a batch isn't retried, and leaves the batch as it was" do
    batch = held_open_batch

    assert_raises(ActiveRecord::Deadlocked) do
      batch.enqueue { ActiveJob.perform_all_later(OrdinaryJob.new("x"), CollectJob.new("c"), DiscardJob.new("a"), TrailingDeadlockJob.new("z")) }
    end

    assert_equal [ :admission ], JobBuffer.values
    assert_batch_only_holds_its_first_job(batch)
  end

  test "a retry keeps its batch and doesn't count again" do
    batch = SolidQueue::Batch.enqueue do
      RetriedJob.perform_later("a")
      2.times { RetriedCollectJob.perform_later("c") }
    end

    perform_ready_jobs
    assert_equal 2, batch.reload.total_jobs
    assert_equal 4, batch.jobs.count
    assert_not batch.finished?

    perform_ready_jobs
    assert_equal [ "a", 2 ], JobBuffer.values.sort_by(&:to_s).reverse
    assert batch.reload.finished?
    assert_equal 2, batch.completed_jobs
  end

  private
    def held_open_batch
      SolidQueue::Batch.enqueue(on_success: CallbackJob) { AddToBufferJob.set(wait: 1.hour).perform_later("held") }
    end

    def assert_batch_only_holds_its_first_job(batch)
      assert_equal 1, batch.reload.total_jobs
      assert_equal 1, batch.jobs.count
      assert_equal 1, SolidQueue::Job.count
      assert_equal 1, SolidQueue::BatchExecution.count
      assert_equal 0, SolidQueue::CoalesceKey.count
      assert_equal 0, SolidQueue::CoalescedCall.count
      assert_not batch.finished?
    end

    def assert_nothing_stored
      assert_equal 0, SolidQueue::Job.count
      assert_equal 0, SolidQueue::BatchExecution.count
      assert_equal 0, SolidQueue::CoalesceKey.count
      assert_equal 0, SolidQueue::CoalescedCall.count
    end
end
