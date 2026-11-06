# frozen_string_literal: true

require "test_helper"

# These run on separate connections and threads on purpose: what they test is
# the locking in admission and start, and any error raised by a thread,
# deadlocks included, fails the test when the thread is joined.
class SolidQueue::CoalesceConcurrencyTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DropJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*)
      RACED_RUNS << provider_job_id
    end
  end

  # Holds the concurrency slot that BlockedDropJob waits for
  class SlotHolderJob < ApplicationJob
    limits_concurrency key: "slot", group: "blocking", to: 1

    def perform(*); end
  end

  class BlockedDropJob < ApplicationJob
    coalesce key: ->(group, *) { group }
    limits_concurrency key: "slot", group: "blocking", to: 1

    def perform(*); end
  end

  RACED_RUNS = Concurrent::Array.new

  setup do
    RACED_RUNS.clear
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "race-worker")
  end

  test "concurrent first calls create one job" do
    5.times do |round|
      results = race(8) { DropJob.perform_later("g#{round}") }

      assert_equal 1, results.count { |active_job| active_job.coalesce_status == :created }
      assert_equal [ results.first.provider_job_id ], results.map(&:provider_job_id).uniq
    end

    assert_equal 5, SolidQueue::Job.count
  end

  test "bulk enqueues racing on overlapping keys create one job per key" do
    race(6) do
      SolidQueue::Job.enqueue_all(%w[ a b c d ].shuffle.flat_map { |group| 3.times.map { DropJob.new(group) } })
    end

    assert_equal 4, SolidQueue::Job.count
    assert_equal 4, SolidQueue::CoalesceKey.count
  end

  test "calls racing starts never leave a key without a job" do
    done = Concurrent::AtomicBoolean.new(false)
    worker = Thread.new { work_until(done) }

    results = begin
      race(6) { 40.times.map { DropJob.perform_later("g") } }.flatten
    ensure
      done.make_true
      worker.join rescue nil
    end
    worker.value
    drain

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal results.count { |active_job| active_job.coalesce_status == :created }, RACED_RUNS.size
  end

  test "a call waiting on a start that deletes the key opens a new job" do
    skip_on_sqlite

    first = DropJob.perform_later("g")
    job = SolidQueue::Job.find(first.provider_job_id)
    locked = Concurrent::Event.new

    starter = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::CoalesceKey.lock_for_release(job.id)
        locked.set
        sleep 0.3
        SolidQueue::Job.find(job.id).start_coalesced_run
      end
    end

    locked.wait(5)
    second = in_thread { DropJob.perform_later("g") }.value
    starter.value

    assert_equal :created, second.coalesce_status
    assert_not_equal job.id, second.provider_job_id
    assert_equal second.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "calls racing discards of the open job don't fail" do
    race(5) do |thread|
      20.times do
        if thread.zero?
          SolidQueue::Job.where(class_name: DropJob.name).find_each(&:discard)
        else
          DropJob.perform_later("g")
        end
      end
    end

    if SolidQueue::CoalesceKey.any?
      assert_equal SolidQueue::CoalesceKey.sole.job_id, SolidQueue::Job.sole.id
    else
      assert_equal 0, SolidQueue::Job.count
    end
  end

  test "bulk discards racing calls don't fail, and whatever isn't discarded still runs" do
    results = race(5) do |thread|
      20.times.map do
        if thread.zero?
          SolidQueue::ReadyExecution.discard_all_in_batches
          nil
        else
          DropJob.perform_later("g")
        end
      end
    end.flatten.compact

    assert results.all? { |active_job| active_job.coalesce_status.in?(%i[ created discarded ]) && active_job.provider_job_id }
    assert_coalesce_invariants

    last_call = DropJob.perform_later("g")
    open_job = SolidQueue::CoalesceKey.sole.job_id
    assert_equal open_job, last_call.provider_job_id

    drain

    assert_includes RACED_RUNS, open_job
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  { scheduled: SolidQueue::ScheduledExecution, blocked: SolidQueue::BlockedExecution, ready: SolidQueue::ReadyExecution }.each do |status, execution_class|
    test "bulk discards of #{status} jobs racing calls don't fail, and leave every key on a live job" do
      job_class, options = status == :blocked ? [ BlockedDropJob, {} ] : [ DropJob, status == :scheduled ? { wait: 1.hour } : {} ]
      SlotHolderJob.perform_later if status == :blocked

      results = race(5) do |thread|
        20.times.map do
          if thread.zero?
            execution_class.discard_all_in_batches
            nil
          else
            job_class.set(options).perform_later("g")
          end
        end
      end.flatten.compact

      assert results.all? { |active_job| active_job.coalesce_status.in?(%i[ created discarded ]) && active_job.provider_job_id }
      assert_coalesce_invariants

      last_call = job_class.set(options).perform_later("g")
      open_job = SolidQueue::Job.find(SolidQueue::CoalesceKey.sole.job_id)
      assert_equal open_job.id, last_call.provider_job_id
      assert_equal status, open_job.status
    end

    test "discards of #{status} jobs one at a time racing calls don't fail" do
      job_class, options = status == :blocked ? [ BlockedDropJob, {} ] : [ DropJob, status == :scheduled ? { wait: 1.hour } : {} ]
      SlotHolderJob.perform_later if status == :blocked

      race(5) do |thread|
        20.times do
          if thread.zero?
            execution_class.where(job_id: SolidQueue::CoalesceKey.select(:job_id)).find_each { |execution| execution.job.discard }
          else
            job_class.set(options).perform_later("g")
          end
        end
      end

      assert_coalesce_invariants
      assert_equal SolidQueue::CoalesceKey.pluck(:job_id), execution_class.where(job_id: SolidQueue::CoalesceKey.select(:job_id)).pluck(:job_id)
    end
  end

  test "bulk discards of failed jobs racing failures and calls don't fail, and leave no key on a failed job" do
    done = Concurrent::AtomicBoolean.new
    failing = in_thread do
      until done.true?
        fail_claimed
        SolidQueue::FailedExecution.discard_all_in_batches
      end
    end

    results = begin
      race(4) { 30.times.map { DropJob.perform_later("g") } }.flatten
    ensure
      done.make_true
    end
    failing.value
    fail_claimed

    assert results.all? { |active_job| active_job.coalesce_status.in?(%i[ created discarded ]) && active_job.provider_job_id }
    assert_equal 0, SolidQueue::ClaimedExecution.count
    assert_coalesce_invariants
  end

  test "a bulk discard that's rolled back keeps the job and its key" do
    active_job = DropJob.perform_later("g")

    SolidQueue::Record.transaction do
      SolidQueue::ReadyExecution.discard_all_in_batches
      assert_equal 0, SolidQueue::CoalesceKey.count
      raise ActiveRecord::Rollback
    end

    assert_coalesce_invariants
    assert_equal active_job.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal :discarded, DropJob.perform_later("g").coalesce_status
  end

  # On the last statement of a discard, so the key's release and the job's delete have
  # to be rolled back
  test "a bulk discard retries a deadlock up to its limit, then raises with nothing lost" do
    active_job = DropJob.perform_later("g")

    deadlocks = deadlocking_on(/\ADELETE FROM .solid_queue_ready_executions/) do
      assert_raises(ActiveRecord::Deadlocked) { SolidQueue::ReadyExecution.discard_all_in_batches }
    end

    assert_equal 6, deadlocks
    assert_equal active_job.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert SolidQueue::Job.exists?(active_job.provider_job_id)
    assert SolidQueue::ReadyExecution.exists?(job_id: active_job.provider_job_id)
  end

  test "failures racing calls don't fail, and leave no key on a failed job" do
    done = Concurrent::AtomicBoolean.new
    failing = in_thread { fail_claimed until done.true? }

    results = race(4) { 30.times.map { DropJob.perform_later("g") } }.flatten
    done.make_true
    failing.value
    fail_claimed

    assert results.all? { |active_job| active_job.coalesce_status.in?(%i[ created discarded ]) && SolidQueue::Job.exists?(active_job.provider_job_id) }
    assert_equal 0, SolidQueue::ClaimedExecution.count
    assert_operator SolidQueue::FailedExecution.count, :>=, 1
    assert_coalesce_invariants
  end

  test "a failure waiting on an admission still releases the key, once the admission is over" do
    skip_on_sqlite

    job, claimed = claim_open_job
    admitted = Concurrent::Event.new
    failing = Concurrent::Event.new

    admission = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::CoalesceKey.admit("#{DropJob.name}/g")
        admitted.set
        failing.wait(5)
        sleep 0.3
      end
    end

    admitted.wait(5)
    failure = in_thread do
      failing.set
      claimed.failed_with(ExpectedTestError.new)
    end

    failure.value
    admission.value

    assert job.reload.failed?
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, DropJob.perform_later("g").coalesce_status
  end

  test "a call waiting on a failure opens a new job when it commits, and joins the job when it rolls back" do
    skip_on_sqlite

    [ true, false ].each do |commit|
      job, claimed = claim_open_job
      failed = Concurrent::Event.new
      calling = Concurrent::Event.new

      failure = in_thread do
        SolidQueue::Record.transaction do
          claimed.failed_with(ExpectedTestError.new)
          failed.set
          calling.wait(5)
          sleep 0.3
          raise ActiveRecord::Rollback unless commit
        end
      end

      failed.wait(5)
      call = in_thread do
        calling.set
        DropJob.perform_later("g")
      end.value
      failure.value

      if commit
        assert_equal :created, call.coalesce_status
        assert_not_equal job.id, call.provider_job_id
        assert job.reload.failed?
      else
        assert_equal :discarded, call.coalesce_status
        assert_equal job.id, call.provider_job_id
        assert_not job.reload.failed?
      end

      SolidQueue::ClaimedExecution.delete_all
      SolidQueue::ReadyExecution.delete_all
      SolidQueue::FailedExecution.delete_all
      SolidQueue::CoalesceKey.delete_all
    end
  end

  test "a deadlock while failing isn't retried, and rolls the release of the key back" do
    job, _claimed = claim_open_job
    released = false
    callback = ->(*, payload) { released ||= payload[:sql].match?(/\ADELETE FROM .solid_queue_coalesce_keys/) }

    deadlocks = ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      deadlocking_on(/\AINSERT INTO .solid_queue_failed_executions/) do
        assert_raises(ActiveRecord::Deadlocked) { job.failed_with(ExpectedTestError.new) }
      end
    end

    assert released, "the key wasn't released before the failure was recorded"
    assert_equal 1, deadlocks
    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id
    assert SolidQueue::ClaimedExecution.exists?(job_id: job.id)
    assert_not job.reload.failed?
    assert_equal job.id, DropJob.perform_later("g").provider_job_id
  end

  test "a deadlock retries the admission up to its limit, then raises" do
    SolidQueue::Job.expects(:create!).times(6).raises(ActiveRecord::Deadlocked)

    assert_raises(SolidQueue::Job::EnqueueError) { DropJob.perform_later("g") }

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a deadlock inside the app's transaction isn't retried" do
    SolidQueue::Job.expects(:create!).once.raises(ActiveRecord::Deadlocked)

    assert_raises(SolidQueue::Job::EnqueueError) do
      # Rails 7.2 holds a perform_later back until the transaction commits
      SolidQueue::Record.transaction { SolidQueue::Job.enqueue(DropJob.new("g")) }
    end

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  private
    def claim_open_job
      job = SolidQueue::Job.find(DropJob.perform_later("g").provider_job_id)
      [ job, SolidQueue::ReadyExecution.claim("*", 1, @process.id).sole ]
    end

    # Claimed but not started, so these still hold their keys, like the jobs of a
    # process that died. Failing isn't retried on a deadlock, so a worker would stop
    # here, and what it didn't finalize stays claimed for the next one to fail.
    def fail_claimed
      SolidQueue::ReadyExecution.claim("*", 2, @process.id)

      SolidQueue::Job.retrying_deadlocks do
        SolidQueue::ClaimedExecution.all.fail_all_with(ExpectedTestError.new)
      end
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
        in_thread do
          barrier.wait(10)
          yield i
        end
      end

      # Let all of them finish, so a failure can't leak writes into the next test
      threads.each { |thread| thread.join rescue nil }
      threads.map(&:value)
    end

    def in_thread(&block)
      Thread.new { SolidQueue::Record.connection_pool.with_connection(&block) }
    end

    def skip_on_sqlite
      skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"
    end
end
