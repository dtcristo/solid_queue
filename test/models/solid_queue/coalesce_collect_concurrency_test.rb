# frozen_string_literal: true

require "test_helper"

# These run on separate connections and threads on purpose: what they test is
# the locking between appends and starts, and any error raised by
# a thread, deadlocks included, fails the test when the thread is joined.
class SolidQueue::CoalesceCollectConcurrencyTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class CollectAllJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 10_000

    def perform_calls(calls)
      RACED_RUNS << [ provider_job_id, calls.map(&:arguments) ]
    end
  end

  class CollectFiveJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 5

    def perform_calls(calls)
      RACED_RUNS << [ provider_job_id, calls.map(&:arguments) ]
    end
  end

  class CollectFiveLimitedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 5
    limits_concurrency key: "shared", to: 1

    def perform_calls(calls)
      RACED_RUNS << [ provider_job_id, calls.map(&:arguments) ]
    end
  end

  class CollectTwoPerCallLimitedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2
    limits_concurrency key: ->(_group, limit_group) { limit_group }, to: 1

    def perform_calls(_calls); end
  end

  class DiscardPerKeyJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class OrdinaryJob < ApplicationJob
    def perform(*); end
  end

  class ChangingModeJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect
  end

  RACED_RUNS = Concurrent::Array.new

  setup do
    RACED_RUNS.clear
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "race-worker")
  end

  test "concurrent first calls produce one open job and keep every collected row" do
    5.times do |round|
      results = race(8) { |thread| 5.times.map { |i| CollectAllJob.perform_later("g#{round}", thread, i) } }.flatten

      assert_equal [ results.first.provider_job_id ], results.map(&:provider_job_id).uniq
      assert_equal 1, results.count { |active_job| active_job.coalesce_status == :created }
      assert results.all?(&:successfully_enqueued?)
      assert_equal 40, SolidQueue::CoalescedCall.where(job_id: results.first.provider_job_id).count
    end

    assert_equal 5, SolidQueue::Job.count
    assert_equal 5, SolidQueue::CoalesceKey.count
  end

  test "bulk enqueues racing on overlapping keys lose nothing" do
    race(6) do |thread|
      SolidQueue::Job.enqueue_all(%w[ a b c d ].shuffle.flat_map { |group| 3.times.map { |i| CollectAllJob.new(group, thread, i) } })
    end

    assert_equal 4, SolidQueue::Job.count
    assert_equal 6 * 4 * 3, SolidQueue::CoalescedCall.waiting.count
  end

  test "concurrent calls never fill a job past its capacity, and leave at most one open" do
    results = race(8) { |thread| 25.times.map { |i| CollectFiveJob.perform_later("g", thread, i) } }.flatten

    assert_jobs_hold_their_calls_within_capacity results, total: 200
    assert_equal 40, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "bulk enqueues racing over job boundaries don't exceed capacity, and each call knows its job" do
    results = race(4) do |thread|
      jobs = 12.times.map { |i| CollectFiveJob.new("g", thread, i) }
      SolidQueue::Job.enqueue_all(jobs)
      jobs
    end.flatten

    assert_jobs_hold_their_calls_within_capacity results, total: 48
    assert_coalesce_invariants
  end

  test "mixed bulk enqueues racing through deadlock retries duplicate and lose nothing" do
    results = deadlocking_every_second_insert_of_each_thread do
      race(6) do |thread|
        jobs = [
          *2.times.map { |i| OrdinaryJob.new(thread, i) },
          *%w[ a b ].flat_map { |group| 2.times.map { DiscardPerKeyJob.new(group) } },
          *%w[ a b ].flat_map { |group| 12.times.map { |i| CollectFiveJob.new(group, thread, i) } }
        ].shuffle

        enqueue_all_recovering(jobs)
        jobs
      end.flatten
    end

    collected = results.grep(CollectFiveJob)
    ordinary = results.grep(OrdinaryJob)
    discarding = results.grep(DiscardPerKeyJob)

    assert_equal collected.map(&:job_id).sort, SolidQueue::CoalescedCall.pluck(:arguments).map { |payload| payload["job_id"] }.sort
    collected.each do |active_job|
      assert_includes SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).map { |row| row.arguments["job_id"] }, active_job.job_id
    end
    assert SolidQueue::CoalescedCall.group(:job_id).count.values.all? { |count| count.between?(1, 5) }

    assert_equal ordinary.map(&:job_id).sort, SolidQueue::Job.where(class_name: OrdinaryJob.name).pluck(:active_job_id).sort

    assert_equal 2, SolidQueue::Job.where(class_name: DiscardPerKeyJob.name).count
    assert_equal 2, discarding.count { |active_job| active_job.coalesce_status == :created }
    assert_equal 22, discarding.count { |active_job| active_job.coalesce_status == :discarded }
  end

  test "another connection can update an earlier job's semaphore during a bulk enqueue" do
    skip_on_sqlite

    jobs = limited_jobs
    update, finished = updating_first_semaphore_at_second_job(jobs) { SolidQueue::Job.enqueue_all(jobs) }

    assert finished, "the update should go through while the enqueue runs"
    assert_equal 1, update.value
    assert_equal 3, jobs.map(&:provider_job_id).uniq.size
  end

  test "inside an enclosing transaction, an earlier job's semaphore stays locked until the commit" do
    skip_on_sqlite

    jobs = limited_jobs
    update, finished = SolidQueue::Record.transaction do
      updating_first_semaphore_at_second_job(jobs, waiting: 0.5) { SolidQueue::Job.enqueue_all(jobs) }
    end

    assert_not finished, "the update should wait for the commit"
    assert_equal 1, update.value
  end

  test "concurrent calls to a class that stopped collecting take over the key from a collecting job once" do
    3.times do |round|
      change_mode_to :collect
      collecting_job = ChangingModeJob.perform_later("g#{round}", :seed).provider_job_id
      change_mode_to :discard

      results = race(6) { |thread| ChangingModeJob.perform_later("g#{round}", thread) }

      assert_equal 1, results.count { |active_job| active_job.coalesce_status == :created }
      assert_not_includes results.map(&:provider_job_id), collecting_job
      assert_coalesce_invariants
    end
  ensure
    change_mode_to :collect
  end

  test "a discard call whose transaction started before the collecting key existed takes it over" do
    skip_on_sqlite

    change_mode_to :collect
    call = nil

    SolidQueue::Job.transaction do
      SolidQueue::Job.count
      Thread.new { ChangingModeJob.perform_later("g", :seed) }.join
      change_mode_to :discard

      call = Timeout.timeout(10) { ChangingModeJob.perform_later("g", :call) }
    end

    assert_equal :created, call.coalesce_status
    assert_equal call.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  ensure
    change_mode_to :collect
  end

  test "concurrent calls to a class that started collecting never join a discarding job" do
    3.times do |round|
      change_mode_to :discard
      discarding_job = ChangingModeJob.perform_later("g#{round}", :seed).provider_job_id
      change_mode_to :collect

      results = race(6) { |thread| ChangingModeJob.perform_later("g#{round}", thread) }

      assert_equal 1, results.map(&:provider_job_id).uniq.size
      assert_not_includes results.map(&:provider_job_id), discarding_job
      assert_equal 0, SolidQueue::CoalescedCall.where(job_id: discarding_job).count
      assert_coalesce_invariants
    end
  ensure
    change_mode_to :collect
  end

  test "simultaneous fills of a nearly full job create one new open job" do
    5.times do |round|
      4.times { |i| CollectFiveJob.perform_later("g#{round}", :seed, i) }
      results = race(5) { |thread| CollectFiveJob.perform_later("g#{round}", thread, 0) }

      assert_equal 1, results.count { |active_job| active_job.coalesce_status == :created }
      assert_equal 2, results.map(&:provider_job_id).uniq.size
      assert_equal 4, SolidQueue::CoalesceKey.find_by!(key: "#{CollectFiveJob.name}/g#{round}").calls_count
    end
  end

  test "a call waiting on the call that seals a job doesn't attach to it" do
    skip_on_sqlite

    4.times { |i| CollectFiveJob.perform_later("g", :seed, i) }
    sealing = Concurrent::Event.new
    sealed = nil

    sealer = in_thread do
      SolidQueue::Record.transaction do
        sealed = SolidQueue::Job.enqueue(CollectFiveJob.new("g", :seal, 0))
        sealing.set
        sleep 0.3
      end
    end

    sealing.wait(5)
    late = in_thread { CollectFiveJob.perform_later("g", :late, 0) }.value
    sealer.value

    assert_equal :created, late.coalesce_status
    assert_equal 5, SolidQueue::CoalescedCall.where.not(job_id: late.provider_job_id).count
    assert_equal late.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalescedCall.where(job_id: late.provider_job_id).count
  end

  test "a sealed job's start doesn't touch a newer open job" do
    5.times { |i| CollectFiveJob.perform_later("g", :sealed, i) }
    sealed_job = SolidQueue::Job.sole
    open_job = CollectFiveJob.set(wait: 1.hour).perform_later("g", :open, 0).provider_job_id

    perform_ready_jobs

    assert_equal [ sealed_job.id ], RACED_RUNS.map(&:first)
    assert_equal 5, RACED_RUNS.sole.last.size
    assert_equal open_job, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
  end

  test "appends racing starts lose nothing, strand nothing and run every row once" do
    calls = race_appends_against_workers(CollectFiveJob, appenders: 6, calls_per_appender: 40, workers: 3)

    assert_every_call_ran_once calls
    assert RACED_RUNS.all? { |_, calls| calls.size <= 5 }
    assert_equal 0, SolidQueue::CoalescedCall.waiting.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_runs_match_their_rows
  end

  test "appends racing starts with several keys and a blocking concurrency limit" do
    calls = race_appends_against_workers(CollectFiveLimitedJob, appenders: 4, calls_per_appender: 30, workers: 3, groups: %w[ a b ])

    assert_every_call_ran_once calls
    assert RACED_RUNS.all? { |_, calls| calls.size <= 5 }
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a start waiting for the key's lock during an enqueue loads that call" do
    skip_on_sqlite

    first = CollectFiveJob.perform_later("g", 0)
    claimed = SolidQueue::ReadyExecution.claim("*", 1, @process.id).sole
    appending = Concurrent::Event.new
    committing = Concurrent::Event.new

    appender = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::Job.enqueue(CollectFiveJob.new("g", 1))
        appending.set
        committing.wait(5)
      end
    end

    appending.wait(5)
    starter = in_thread { claimed.perform }

    wait_for_a_lock_wait
    committing.set

    appender.value
    starter.value

    assert_equal [ [ first.provider_job_id, [ [ "g", 0 ], [ "g", 1 ] ] ] ], RACED_RUNS.to_a
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "an append waiting on a start that deletes the key opens a new job" do
    skip_on_sqlite

    first = CollectFiveJob.perform_later("g", 0)
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
    second = in_thread { CollectFiveJob.perform_later("g", 1) }.value
    starter.value

    assert_equal :created, second.coalesce_status
    assert_not_equal job.id, second.provider_job_id
    assert_equal 1, SolidQueue::CoalescedCall.where(job_id: second.provider_job_id).count
    assert_equal second.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "a bulk discard waits for an admission holding the key, instead of deadlocking with it" do
    skip_on_sqlite

    first = CollectFiveJob.perform_later("g", 0)
    job_id = first.provider_job_id
    admitted = Concurrent::Event.new
    discarding = Concurrent::Event.new

    admission = in_thread do
      SolidQueue::Record.transaction do
        key = SolidQueue::CoalesceKey.admit_exclusively("#{CollectFiveJob.name}/g")
        admitted.set
        discarding.wait(5)
        sleep 0.3
        SolidQueue::CoalescedCall.insert_all([ { job_id: key.job_id, arguments: CollectFiveJob.new("g", 1).serialize } ])
        key.fill(key.job_id, 1, 5)
      end
    end

    admitted.wait(5)
    discard = in_thread do
      discarding.set
      SolidQueue::ReadyExecution.discard_all_from_jobs(SolidQueue::Job.where(id: job_id))
    end

    discard.value
    admission.value

    assert_equal 0, SolidQueue::Job.where(id: job_id).count
    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "bulk discards racing calls don't fail, and never leave a key or a call without its job" do
    results = race(5) do |thread|
      20.times.map do |i|
        if thread.zero?
          SolidQueue::ReadyExecution.discard_all_in_batches
          nil
        else
          CollectFiveJob.perform_later("g", thread, i)
        end
      end
    end

    assert results.flatten.compact.all?(&:successfully_enqueued?)
    assert_equal [], SolidQueue::CoalesceKey.pluck(:job_id) - SolidQueue::Job.pluck(:id)
    assert_equal [], SolidQueue::CoalescedCall.pluck(:job_id).uniq - SolidQueue::Job.pluck(:id)
  end

  test "bulk discards racing calls keep the calls of whatever isn't discarded" do
    results = race(5) do |thread|
      20.times.map do |i|
        if thread.zero?
          SolidQueue::ReadyExecution.discard_all_in_batches
          nil
        else
          CollectAllJob.perform_later("g", thread, i)
        end
      end
    end.flatten.compact

    assert results.all?(&:successfully_enqueued?)
    assert_coalesce_invariants

    kept = SolidQueue::CoalescedCall.pluck(:arguments).map { |payload| payload["job_id"] }
    assert_empty kept - results.map(&:job_id)
    drain_ready
    assert_equal kept.size, RACED_RUNS.flat_map(&:last).size
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a bulk discard that's rolled back keeps the job, its key and its calls" do
    active_job = CollectAllJob.perform_later("g", 1)

    SolidQueue::Record.transaction do
      SolidQueue::ReadyExecution.discard_all_in_batches
      assert_equal 0, SolidQueue::CoalescedCall.count
      raise ActiveRecord::Rollback
    end

    assert_coalesce_invariants
    assert_equal active_job.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).count
  end

  # On the last statement of a discard, so the key's release and the job's delete, with
  # its calls, have to be rolled back
  test "a bulk discard retries a deadlock up to its limit, then raises with nothing lost" do
    active_job = CollectAllJob.perform_later("g", 1)

    deadlocks = deadlocking_on(/\ADELETE FROM .solid_queue_ready_executions/) do
      assert_raises(ActiveRecord::Deadlocked) { SolidQueue::ReadyExecution.discard_all_in_batches }
    end

    assert_equal 6, deadlocks
    assert_equal active_job.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
    assert_equal 1, SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).count
    assert SolidQueue::ReadyExecution.exists?(job_id: active_job.provider_job_id)
  end

  test "failures racing calls lose no call, and leave no key on a failed job" do
    done = Concurrent::AtomicBoolean.new
    failing = in_thread { fail_claimed until done.true? }

    # 31 each, so the last job isn't full and has a key left to release
    results = race(4) { |thread| 31.times.map { |i| CollectFiveJob.perform_later("g", thread, i) } }.flatten
    done.make_true
    failing.value
    fail_claimed

    assert results.all?(&:successfully_enqueued?)
    assert_equal results.map(&:job_id).sort, SolidQueue::CoalescedCall.pluck(:arguments).map { |payload| payload["job_id"] }.sort
    results.each do |active_job|
      assert_includes SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).map { |row| row.arguments["job_id"] }, active_job.job_id
    end
    assert_equal 0, SolidQueue::ClaimedExecution.count
    assert_coalesce_invariants
  end

  test "a deadlock while failing isn't retried, and leaves the key and the calls where they were" do
    active_job = CollectAllJob.perform_later("g", 1)
    claimed = SolidQueue::ReadyExecution.claim("*", 1, @process.id).sole
    SolidQueue::FailedExecution.expects(:create!).once.raises(ActiveRecord::Deadlocked)

    assert_raises(ActiveRecord::Deadlocked) { claimed.failed_with(ExpectedTestError.new) }

    assert_equal active_job.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
    assert_equal 1, SolidQueue::CoalescedCall.count
    assert SolidQueue::ClaimedExecution.exists?(job_id: active_job.provider_job_id)
  end

  test "appends racing discards of the open job don't fail, and every append ends up on a job" do
    rounds = 20
    results = race(5) do |thread|
      rounds.times.map do |i|
        if thread.zero?
          SolidQueue::Job.where(class_name: CollectAllJob.name).find_each(&:discard)
          nil
        else
          CollectAllJob.perform_later("g", thread, i)
        end
      end
    end

    assert results.flatten.compact.all?(&:successfully_enqueued?)

    # Whatever wasn't discarded is on the last open job
    if SolidQueue::CoalesceKey.any?
      assert_equal SolidQueue::CoalesceKey.sole.job_id, SolidQueue::Job.sole.id
      assert_equal SolidQueue::CoalesceKey.sole.calls_count, SolidQueue::CoalescedCall.where(job_id: SolidQueue::Job.sole.id).count
    else
      assert_equal 0, SolidQueue::CoalescedCall.count
    end
  end

  test "calls racing discards of filling jobs never leave a key or a call without its job" do
    race(5) do |thread|
      30.times do |i|
        if thread.zero?
          SolidQueue::Job.where(class_name: CollectFiveJob.name).find_each(&:discard)
        else
          CollectFiveJob.perform_later("g", thread, i)
        end
      end
    end

    assert_equal 0, SolidQueue::CoalesceKey.where.not(job_id: SolidQueue::Job.select(:id)).count
    assert_equal 0, SolidQueue::CoalescedCall.where.not(job_id: SolidQueue::Job.select(:id)).count
    assert SolidQueue::Job.all.all? { |job| job.coalesced_calls.count.between?(1, 5) }
  end

  private
    def change_mode_to(on_conflict)
      ChangingModeJob.coalesce key: ->(group, *) { group }, on_conflict: on_conflict
    end

    # Four calls in two jobs on one key, and a third job on another. Each job
    # has its own semaphore, already there, so that holding one means locking its row.
    def limited_jobs
      [ %w[ g s1 ], %w[ g s1 ], %w[ g s2 ], %w[ g s2 ], %w[ h s3 ] ].map { |arguments| CollectTwoPerCallLimitedJob.new(*arguments) }.tap do |jobs|
        jobs.map(&:concurrency_key).uniq.each { |key| SolidQueue::Semaphore.create!(key: key, value: 1, expires_at: 1.hour.from_now) }
      end
    end

    # Subscribers run on the enqueuing thread, between one job's transaction and
    # the next, so the update always starts at that point, and the enqueue waits for
    # it. Returns the update's thread, with its row count as its value, and whether
    # it finished in time. One that's still waiting finishes when the locks go away.
    def updating_first_semaphore_at_second_job(jobs, waiting: 5)
      update = finished = nil
      created = 0
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
        if payload[:status] == :created && (created += 1) == 2
          update = in_thread { SolidQueue::Semaphore.where(key: jobs.first.concurrency_key).update_all(value: 0) }
          finished = update.join(waiting).present?
        end
      end

      yield
      [ update, finished ]
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    # InnoDB's own deadlocks can use up a call's retries here, on top of the ones
    # this test adds, and the README's recovery has to hold up when they do.
    def enqueue_all_recovering(jobs, attempts: 10)
      SolidQueue::Job.enqueue_all(jobs)
    rescue SolidQueue::Job::EnqueueError, ActiveRecord::Deadlocked
      raise if (attempts -= 1).zero?

      jobs = jobs.reject(&:provider_job_id)
      retry
    end

    # Runs each insert before raising, so there's a real transaction to roll back,
    # on the same attempts every time. Admission retries a deadlock up to 5 times.
    def deadlocking_every_second_insert_of_each_thread
      insert_all = SolidQueue::CoalescedCall.method(:insert_all)

      SolidQueue::CoalescedCall.define_singleton_method(:insert_all) do |rows, **options|
        attempt = Thread.current[:coalesce_test_insert_attempts] = Thread.current[:coalesce_test_insert_attempts].to_i + 1
        insert_all.call(rows, **options).tap { raise ActiveRecord::Deadlocked, "boom" if attempt.even? }
      end

      yield
    ensure
      SolidQueue::CoalescedCall.singleton_class.remove_method(:insert_all)
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

    def drain_ready
      nil until SolidQueue::ReadyExecution.claim("*", 2, @process.id).each(&:perform).empty?
    end

    def race_appends_against_workers(job_class, appenders:, calls_per_appender:, workers:, groups: %w[ g ])
      done = Concurrent::AtomicBoolean.new(false)
      worker_threads = workers.times.map { Thread.new { work_until(done) } }

      appended = begin
        race(appenders) do |thread|
          calls_per_appender.times.map do |i|
            call = [ groups.sample, thread, i ]
            job_class.perform_later(*call)
            call
          end
        end.flatten(1)
      ensure
        done.make_true
        worker_threads.each { |thread| thread.join rescue nil }
      end

      worker_threads.each(&:value)
      drain

      appended
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
      loop do
        SolidQueue::BlockedExecution.unblock(100)
        SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.second.ago)
        SolidQueue::ScheduledExecution.dispatch_next_batch(100)
        break if perform_claimed.empty? && SolidQueue::ScheduledExecution.none? && SolidQueue::BlockedExecution.none? && SolidQueue::ReadyExecution.none?
      end
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

    # Until a connection to this database is waiting for a row lock, which fails the
    # test if it doesn't happen
    def wait_for_a_lock_wait(timeout: 5)
      waiting = if SolidQueue::Record.connection_pool.db_config.adapter == "postgresql"
        "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()"
      else
        # Not information_schema.innodb_trx, which InnoDB doesn't refresh while it's read more often than every 100ms
        "SELECT COUNT(*) FROM performance_schema.data_lock_waits JOIN performance_schema.data_locks ON data_locks.engine_lock_id = data_lock_waits.requesting_engine_lock_id WHERE data_locks.object_schema = DATABASE()"
      end

      in_thread do |connection|
        Timeout.timeout(timeout) { sleep 0.01 until connection.select_value(waiting).to_i.positive? }
      end.value
    end

    def assert_every_call_ran_once(calls)
      assert_equal calls.sort_by(&:to_s), RACED_RUNS.flat_map(&:last).sort_by(&:to_s)
    end

    def assert_jobs_hold_their_calls_within_capacity(results, total:)
      assert results.all?(&:successfully_enqueued?)

      counts = SolidQueue::CoalescedCall.group(:job_id).count
      assert_equal total, counts.values.sum
      assert counts.values.all? { |count| count.between?(1, 5) }, counts.to_s
      assert_operator counts.values.count { |count| count < 5 }, :<=, 1
      assert_operator SolidQueue::CoalesceKey.count, :<=, 1

      counts.each_key { |job_id| assert SolidQueue::Job.exists?(job_id) }
      results.each do |active_job|
        assert_includes SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).map { |row| row.arguments["job_id"] }, active_job.job_id
      end
    end

    def assert_runs_match_their_rows
      RACED_RUNS.each do |job_id, calls|
        rows = SolidQueue::CoalescedCall.where(job_id: job_id).order(:id).map { |row| row.arguments["arguments"] }
        assert_equal rows, calls
      end
    end

    def skip_on_sqlite
      skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"
    end
end
