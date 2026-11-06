# frozen_string_literal: true

require "test_helper"

# Coalesced jobs as members of a SolidQueue::Batch. Solid Queue's accounting stays
# authoritative, so these look at the batch's jobs, counters and callbacks.
class SolidQueue::CoalesceBatchTest < ActiveSupport::TestCase
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

  class LimitedJob < ApplicationJob
    coalesce key: ->(group) { group }
    limits_concurrency to: 1, key: ->(group) { group }, on_conflict: :block, duration: 5.minutes

    def perform(group)
      JobBuffer.add(group)
    end
  end

  class LimitedDiscardingJob < ApplicationJob
    coalesce key: ->(group) { group }
    limits_concurrency to: 1, key: ->(group) { group }, on_conflict: :discard, duration: 5.minutes

    def perform(*); end
  end

  class FailingJob < ApplicationJob
    coalesce key: ->(group) { group }

    def perform(*)
      raise ExpectedTestError
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

  class SelfAddingJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(group, call)
      JobBuffer.add(call)
      batch.enqueue { self.class.perform_later(group, :during_run) } if call == :first
    end
  end

  class ReadsBatchJob < ApplicationJob
    coalesce key: ->(group) { group }

    def perform(*)
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

  class DeferredJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :always : true if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group) { group }

    def perform(group)
      JobBuffer.add(group)
    end
  end

  class InlineJob < ApplicationJob
    self.queue_adapter = :inline
    coalesce key: ->(group, *) { group }

    def perform(_group, call)
      JobBuffer.add(call)
    end
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

  class RacedJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(group, *)
      RACED_RUNS << [ batch_id, group ]
    end
  end

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

    assert_equal [ "Batch/#{batch.id}/SolidQueue::CoalesceBatchTest::DiscardJob/a" ] * 3, events.map { |payload| payload[:key] }
    assert_equal [ :created, :discarded, :discarded ], calls.map(&:coalesce_status)
    assert_equal [ batch.jobs.sole.id ], calls.map(&:provider_job_id).uniq
    assert_equal 1, batch.reload.total_jobs

    perform_ready_jobs

    assert_equal [ [ "a", 0 ] ], JobBuffer.values
    assert batch.reload.finished?
    assert_equal 1, batch.completed_jobs
  end

  test "each batch has its own job for a key, and so do calls outside a batch" do
    first = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a", :first) }
    second = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a", :second) }
    outside = DiscardJob.perform_later("a", :outside)
    in_batch_after_outside = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a", :third) }

    assert_equal :created, outside.coalesce_status
    assert_nil SolidQueue::Job.find(outside.provider_job_id).batch_id
    assert_equal 4, SolidQueue::Job.count
    [ first, second, in_batch_after_outside ].each { |batch| assert_equal 1, batch.reload.total_jobs }

    assert_equal :discarded, DiscardJob.perform_later("a", :outside_again).coalesce_status
    assert_equal 4, SolidQueue::Job.count
  end

  test "a batch's callbacks fire once, after its last job" do
    batch = SolidQueue::Batch.enqueue(on_success: CallbackJob, on_finish: CallbackJob) do
      2.times { DiscardJob.perform_later("a") }
      DiscardJob.perform_later("b")
    end

    assert_not batch.reload.finished?

    perform_ready_jobs
    assert batch.reload.finished?
    assert_equal 2, batch.completed_jobs

    perform_ready_jobs
    assert_equal 2, JobBuffer.values.count(:callback)
    assert_empty perform_ready_jobs
  end

  test "a call from a running job joins the open job, and one after it starts opens another" do
    batch = SolidQueue::Batch.enqueue do
      SelfAddingJob.perform_later("a", :first)
    end

    perform_ready_jobs
    assert_equal 2, batch.reload.total_jobs
    assert_not batch.finished?

    batch.enqueue { SelfAddingJob.perform_later("a", :joins) }
    assert_equal 2, batch.reload.total_jobs

    perform_ready_jobs
    assert_equal [ :first, :during_run ], JobBuffer.values
    assert batch.reload.finished?
  end

  test "a job can read its batch in perform" do
    batch = SolidQueue::Batch.enqueue { ReadsBatchJob.perform_later("a") }

    perform_ready_jobs

    assert_equal [ batch.id ], JobBuffer.values
  end

  test "limits_concurrency applies across batches" do
    first = SolidQueue::Batch.enqueue { LimitedJob.perform_later("a") }
    second = SolidQueue::Batch.enqueue { LimitedJob.perform_later("a") }

    assert first.jobs.sole.ready?
    assert second.jobs.sole.blocked?

    perform_ready_jobs
    assert first.reload.finished?
    assert_not second.reload.finished?

    perform_ready_jobs
    assert second.reload.finished?
    assert_equal [ "a", "a" ], JobBuffer.values
  end

  test "a job a concurrency conflict discards counts as completed, and frees its key" do
    SolidQueue::Batch.enqueue { LimitedDiscardingJob.perform_later("a") }

    call = nil
    batch = SolidQueue::Batch.enqueue { call = LimitedDiscardingJob.perform_later("a") }

    assert_nil call.provider_job_id
    assert_equal 1, SolidQueue::CoalesceKey.count
    assert batch.reload.finished?
    assert_equal 1, batch.total_jobs
    assert_equal 1, batch.completed_jobs
  end

  test "a coalesced callback job stays outside the batch" do
    SolidQueue::Batch.enqueue(on_success: CoalescedCallbackJob) { DiscardJob.perform_later("a") }

    perform_ready_jobs

    callback = SolidQueue::Job.find_by!(finished_at: nil)
    assert_nil callback.batch_id
    assert_equal callback.id, CoalescedCallbackJob.perform_later.provider_job_id
  end

  test "perform_all_later in a batch coalesces and mixes with ordinary jobs" do
    calls = [ DiscardJob.new("a", 1), DiscardJob.new("a", 2), DiscardJob.new("c", 3), OrdinaryJob.new("x") ]

    batch = SolidQueue::Batch.enqueue { ActiveJob.perform_all_later(calls) }

    assert_equal [ :created, :discarded, :created, nil ], calls.map(&:coalesce_status)
    assert_equal 3, batch.reload.total_jobs
    assert_equal 3, batch.jobs.count
    assert_not batch.finished?

    perform_ready_jobs

    assert batch.reload.finished?
    assert_equal 3, batch.completed_jobs
  end

  test "jobs built in different batches and enqueued together stay in their batches" do
    jobs = []
    held = AddToBufferJob.set(wait: 1.hour)
    first = SolidQueue::Batch.enqueue { held.perform_later("held"); jobs << DiscardJob.new("a", 1) }
    second = SolidQueue::Batch.enqueue { held.perform_later("held"); jobs << DiscardJob.new("a", 2) }

    ActiveJob.perform_all_later(jobs)

    assert_equal 2, first.reload.total_jobs
    assert_equal 2, second.reload.total_jobs
    assert_equal [ :created, :created ], jobs.map(&:coalesce_status)
  end

  test "a job built in a batch and enqueued later keeps that batch" do
    job = nil
    batch = SolidQueue::Batch.enqueue do
      AddToBufferJob.set(wait: 1.hour).perform_later("held")
      job = DiscardJob.new("a")
    end

    job.enqueue

    assert_equal batch.id, SolidQueue::Job.find(job.provider_job_id).batch_id
    assert_equal 2, batch.reload.total_jobs
  end

  test "enqueuing in another batch moves a job to that batch" do
    single, bulk = nil
    SolidQueue::Batch.enqueue do
      AddToBufferJob.set(wait: 1.hour).perform_later("held")
      single = DiscardJob.new("a")
      bulk = DiscardJob.new("b")
    end

    other = SolidQueue::Batch.enqueue do
      single.enqueue
      ActiveJob.perform_all_later(bulk)
    end

    assert_equal [ other.id, other.id ], SolidQueue::Job.where(id: [ single.provider_job_id, bulk.provider_job_id ]).pluck(:batch_id)
    assert_equal 2, other.reload.total_jobs
  end

  test "enqueuing into a finished batch raises and holds no key" do
    single, bulk = nil
    batch = SolidQueue::Batch.enqueue do
      single = DiscardJob.new("a")
      bulk = DiscardJob.new("b")
    end
    assert batch.reload.finished?

    assert_raises(SolidQueue::Batch::AlreadyFinished) { single.enqueue }
    assert_raises(SolidQueue::Batch::AlreadyFinished) { ActiveJob.perform_all_later(bulk) }

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a block that raises rolls back the batch and everything in it" do
    assert_raises(ExpectedTestError) do
      SolidQueue::Batch.enqueue(on_success: CallbackJob) do
        DiscardJob.perform_later("a")
        DiscardJob.perform_later("c")
        raise ExpectedTestError
      end
    end

    assert_nothing_stored
    assert_equal 0, SolidQueue::Batch.count
  end

  test "a failure partway through a mixed bulk enqueue leaves the batch as it was" do
    batch = held_open_batch

    assert_raises(ExpectedTestError) do
      batch.enqueue { ActiveJob.perform_all_later(OrdinaryJob.new("x"), DiscardJob.new("c"), DiscardJob.new("a"), FailingAdmissionJob.new("z")) }
    end

    assert_batch_only_holds_its_first_job(batch)
  end

  test "a deadlock inside a batch isn't retried, and leaves the batch as it was" do
    batch = held_open_batch

    assert_raises(ActiveRecord::Deadlocked) do
      batch.enqueue { ActiveJob.perform_all_later(OrdinaryJob.new("x"), DiscardJob.new("c"), DiscardJob.new("a"), TrailingDeadlockJob.new("z")) }
    end

    assert_equal [ :admission ], JobBuffer.values
    assert_batch_only_holds_its_first_job(batch)
  end

  test "a failed job counts as failed and frees its key" do
    batch = SolidQueue::Batch.enqueue(on_failure: CallbackJob) { FailingJob.perform_later("a") }

    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert batch.reload.finished?
    assert batch.failed?
    assert_equal 1, batch.failed_jobs
    assert_equal 0, SolidQueue::CoalesceKey.count

    perform_ready_jobs
    assert_equal [ :callback ], JobBuffer.values
  end

  test "a retry keeps its batch and doesn't count again" do
    batch = SolidQueue::Batch.enqueue do
      2.times { RetriedJob.perform_later("a") }
      RetriedJob.perform_later("b")
    end

    perform_ready_jobs
    assert_equal 2, batch.reload.total_jobs
    assert_equal 4, batch.jobs.count
    assert_not batch.finished?

    perform_ready_jobs
    assert_equal [ "a", "b" ], JobBuffer.values.sort
    assert batch.reload.finished?
    assert_equal 2, batch.completed_jobs
  end

  test "discarding a job frees its key and lets the batch finish" do
    batch = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a") }

    batch.jobs.sole.discard

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert batch.reload.finished?
  end

  test "a bulk discard frees the key and leaves the batch for the sweep" do
    batch = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a") }

    SolidQueue::ReadyExecution.discard_all_from_jobs(batch.jobs.to_a)

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_not batch.reload.finished?

    SolidQueue::Batch.sweep_stalled

    assert batch.reload.finished?
  end

  test "a batch finishes when finished jobs aren't preserved" do
    preserve_finished_jobs, SolidQueue.preserve_finished_jobs = SolidQueue.preserve_finished_jobs, false
    batch = SolidQueue::Batch.enqueue { DiscardJob.perform_later("a") }

    perform_ready_jobs

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert batch.reload.finished?
  ensure
    SolidQueue.preserve_finished_jobs = preserve_finished_jobs
  end

  test "a deferred call keeps its batch and coalesces when it commits" do
    skip_without_enqueue_after_transaction_commit

    batch = SolidQueue::Batch.enqueue { 2.times { DeferredJob.perform_later("a") } }

    assert_equal 1, batch.reload.total_jobs
    assert_equal batch.id, SolidQueue::Job.sole.batch_id
    assert_not batch.finished?

    perform_ready_jobs

    assert_equal [ "a" ], JobBuffer.values
    assert batch.reload.finished?
  end

  test "a deferred bulk enqueue only keeps a batch for jobs built inside it" do
    skip "Needs a deferred perform_all_later" unless defined?(ActiveJob::EnqueueAfterTransactionCommit::ActiveJobMethods)

    outside = DeferredJob.new("outside")
    inside = nil
    batch = SolidQueue::Batch.enqueue do
      AddToBufferJob.set(wait: 1.hour).perform_later("held")
      inside = DeferredJob.new("inside")
      ActiveJob.perform_all_later(inside, outside)
    end

    assert_equal batch.id, SolidQueue::Job.find(inside.provider_job_id).batch_id
    assert_nil SolidQueue::Job.find(outside.provider_job_id).batch_id
  end

  test "other adapters run each call on its own and add nothing to the batch" do
    batch = SolidQueue::Batch.enqueue { 2.times { |i| InlineJob.perform_later("a", i) } }

    assert_equal [ 0, 1 ], JobBuffer.values
    assert_equal 0, batch.reload.total_jobs
    assert_equal 0, SolidQueue::Job.count
  end

  test "calls racing workers in one batch count each job once and run every one" do
    skip_on_sqlite

    batch = held_open_batch
    done = Concurrent::AtomicBoolean.new(false)
    workers = 2.times.map { Thread.new { work_until(done) } }

    begin
      race(4) do |thread|
        20.times { |i| enqueue_recovering(batch.id) { RacedJob.perform_later("a", thread, i) } }
      end
    ensure
      done.make_true
      workers.each { |worker| worker.join rescue nil }
    end
    workers.each(&:value)
    drain

    assert_coalesce_invariants
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal batch.jobs.count, batch.reload.total_jobs
    assert_equal batch.jobs.count - 1, RACED_RUNS.size
    assert_equal [ batch.id ], RACED_RUNS.map(&:first).uniq
    assert_not batch.finished?

    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.second.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    drain
    assert batch.reload.finished?
  end

  test "concurrent batch.enqueue calls with overlapping keys keep the counters right" do
    skip_on_sqlite

    batch = held_open_batch

    race(2) do |thread|
      enqueue_recovering(batch.id) do
        (thread.zero? ? %w[ a b c ] : %w[ c b a ]).each { |group| RacedJob.perform_later(group) }
      end
    end

    assert_coalesce_invariants
    assert_equal 3, SolidQueue::CoalesceKey.count
    assert_equal 4, batch.reload.total_jobs
    assert_equal 4, batch.jobs.count
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
      assert_not batch.finished?
    end

    def assert_nothing_stored
      assert_equal 0, SolidQueue::Job.count
      assert_equal 0, SolidQueue::BatchExecution.count
      assert_equal 0, SolidQueue::CoalesceKey.count
    end

    # Admission can't retry a deadlock inside a batch's transaction, so its caller does
    def enqueue_recovering(batch_id, attempts: 20, &block)
      SolidQueue::Batch.find(batch_id).enqueue(&block)
    rescue SolidQueue::Job::EnqueueError => error
      raise unless error.message.include?("Deadlocked") && (attempts -= 1) > 0
      retry
    end

    def work_until(done)
      SolidQueue::Record.connection_pool.with_connection do
        until done.true?
          perform_claimed
          sleep 0.005
        end
      end
    end

    def drain
      nil until perform_claimed.empty? && SolidQueue::ReadyExecution.none?
    end

    def perform_claimed
      SolidQueue::ReadyExecution.claim("*", 2, @process.id).each(&:perform)
    end

    # Runs the block in that many threads with a connection each, all starting
    # at once, and returns their results. Thread#value re-raises their errors.
    def race(count)
      barrier = Concurrent::CyclicBarrier.new(count)

      threads = count.times.map do |i|
        Thread.new do
          SolidQueue::Record.connection_pool.with_connection do
            barrier.wait(10)
            yield i
          end
        end
      end

      # Let all of them finish, so a failure can't leak writes into the next test
      threads.each { |thread| thread.join rescue nil }
      threads.map(&:value)
    end

    def skip_without_enqueue_after_transaction_commit
      skip "Needs Rails 7.2" unless ActiveJob::Base.respond_to?(:enqueue_after_transaction_commit=)
    end

    def skip_on_sqlite
      skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"
    end
end
