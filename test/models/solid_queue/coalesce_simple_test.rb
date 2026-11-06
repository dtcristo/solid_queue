# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceSimpleTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class AnotherRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }

    def perform(name)
      JobBuffer.add(name)
    end
  end

  class KeyedJob < ApplicationJob
    coalesce key: ->(value) { value }

    def perform(_value); end
  end

  class OtherKeyedJob < ApplicationJob
    coalesce key: ->(value) { value }

    def perform(_value); end
  end

  class DeferredRefreshJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :always : true if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(name) { name }
    after_enqueue { JobBuffer.add(coalesce_status) }

    def perform(name)
    end
  end

  class CollectingRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }, on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls.size)
    end
  end

  class OrdinaryFailingJob < ApplicationJob
    def perform
      raise ExpectedTestError
    end
  end

  class StoppedRefreshJob < ApplicationJob
    def perform(name)
      JobBuffer.add(name)
    end
  end

  class RetriedRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    retry_on ExpectedTestError, wait: 0.seconds

    def perform(name)
      JobBuffer.add(name)
      raise ExpectedTestError if executions == 1
    end
  end

  class AbortedRetryRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    retry_on ExpectedTestError, wait: 0.seconds
    before_enqueue { throw :abort if executions > 0 }

    def perform(name)
      raise ExpectedTestError
    end
  end

  class RetriedDiscardingRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    limits_concurrency key: ->(name) { name }, on_conflict: :discard
    retry_on ExpectedTestError, wait: 0.seconds

    def perform(name)
      raise ExpectedTestError
    end
  end

  class LimitedRefreshJob < ApplicationJob
    coalesce key: ->(name, *) { name }
    limits_concurrency key: ->(_name, group) { group }

    def perform(name, _group)
      JobBuffer.add(name)
    end
  end

  class DiscardingRefreshJob < ApplicationJob
    coalesce key: ->(name, *) { name }
    limits_concurrency key: ->(_name, group) { group }, on_conflict: :discard

    def perform(name, _group)
      JobBuffer.add(name)
    end
  end

  class RecordKeyedRefreshJob < ApplicationJob
    coalesce key: ->(result) { result }

    def perform(result)
      JobBuffer.add(result)
    end
  end

  class InlineRefreshJob < ApplicationJob
    self.queue_adapter = :inline

    coalesce key: ->(name) { name }

    def perform(name)
      JobBuffer.add(name)
    end
  end

  class StatusInCallbackJob < ApplicationJob
    coalesce key: ->(name) { name }

    after_enqueue { JobBuffer.add(coalesce_status) }

    def perform(name)
    end
  end

  class KeywordRefreshJob < ApplicationJob
    coalesce key: ->(name, **) { name }

    def perform(name, force:)
      JobBuffer.add([ name, force ])
    end
  end

  class StringKeyedRefreshJob < ApplicationJob
    coalesce key: "everything"

    def perform(*)
    end
  end

  test "an integer and its string give the same key" do
    assert_equal [ :created, :discarded ], [ 1, "1" ].map { |value| KeyedJob.perform_later(value).coalesce_status }
  end

  test "an array's items make a composite key, and a slash in an item collides with two items" do
    assert_equal [ :created, :discarded, :created ], [ [ "a", "b" ], [ "a/b" ], [ "a", "c" ] ].map { |value| KeyedJob.perform_later(value).coalesce_status }
  end

  test "two classes can't share a key" do
    assert_equal [ :created, :created ], [ KeyedJob, OtherKeyedJob ].map { |job_class| job_class.perform_later("a").coalesce_status }
  end

  test "a key Proc that returns nil merges all of those calls" do
    first, second = [ nil, nil ].map { |value| KeyedJob.perform_later(value) }

    assert_equal [ :created, :discarded ], [ first.coalesce_status, second.coalesce_status ]
  end

  test "the first call creates a job, and says so" do
    events = capture_events("coalesce.solid_queue") do
      assert_difference -> { SolidQueue::Job.count }, +1 do
        active_job = CoalescedRefreshJob.perform_later("a")

        assert active_job.successfully_enqueued?
        assert_equal :created, active_job.coalesce_status
        assert_equal SolidQueue::Job.last.id, active_job.provider_job_id
      end
    end

    assert_equal({ status: :created, key: "CoalescedRefreshJob/a", job_id: SolidQueue::Job.last.id, active_job_id: SolidQueue::Job.last.active_job_id, class_name: "CoalescedRefreshJob" }, events.sole.payload)
  end

  test "coalesce_status is nil until an enqueue, and for perform_now" do
    assert_nil CoalescedRefreshJob.new("a").coalesce_status

    active_job = CoalescedRefreshJob.new("a")
    active_job.perform_now
    assert_nil active_job.coalesce_status
  end

  test "coalesce_status is nil on another adapter" do
    active_job = InlineRefreshJob.perform_later("a")

    assert_nil active_job.coalesce_status
    assert_equal [ "a" ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "coalesce_status is the only outcome an Active Job exposes, and it's read-only" do
    active_job = CoalescedRefreshJob.perform_later("a")

    assert_raises(NoMethodError) { active_job.coalesce_status = :discarded }
    assert_equal :created, active_job.coalesce_status
    assert_not_respond_to active_job, :coalesced?
  end

  test "a Solid Queue job doesn't expose whether its class coalesces" do
    assert_not_respond_to SolidQueue::Job.new, :coalesced?
  end

  test "after_enqueue sees the outcome of a single enqueue" do
    StatusInCallbackJob.perform_later("a")
    StatusInCallbackJob.perform_later("a")

    assert_equal [ :created, :discarded ], JobBuffer.values
  end

  test "a discarding job performs with its own arguments" do
    KeywordRefreshJob.perform_later("a", force: true)

    perform_ready_jobs

    assert_equal [ [ "a", true ] ], JobBuffer.values
  end

  test "the first call's queue, priority and arguments win" do
    CoalescedRefreshJob.set(queue: "first", priority: 7).perform_later("a", "first")
    CoalescedRefreshJob.set(queue: "second", priority: 1).perform_later("a", "second")

    job = SolidQueue::Job.sole
    assert_equal "first", job.queue_name
    assert_equal 7, job.priority
    assert_equal [ "a", "first" ], job.arguments["arguments"]
  end

  test "perform_all_later counts discarded calls as not enqueued and reports them with events, skipping enqueue callbacks" do
    jobs = [ StatusInCallbackJob.new("bulk"), StatusInCallbackJob.new("bulk") ]

    events = capture_events("coalesce.solid_queue") { ActiveJob.perform_all_later(jobs) }

    assert_equal [ true, false ], jobs.map(&:successfully_enqueued?)
    assert_equal [ :created, :discarded ], jobs.map(&:coalesce_status)
    assert_equal SolidQueue::Job.sole.id, jobs.map(&:provider_job_id).uniq.sole
    assert_equal [ :created, :discarded ], events.map { |event| event.payload[:status] }
    assert_equal jobs.map(&:job_id), events.map { |event| event.payload[:active_job_id] }
    assert_empty JobBuffer.values
  end

  test "no event and no status when a concurrency conflict destroys the new job" do
    DiscardingRefreshJob.perform_later("a", "group")

    discarded = nil
    events = capture_events("coalesce.solid_queue") { discarded = DiscardingRefreshJob.perform_later("b", "group") }

    assert_nil discarded.coalesce_status
    assert_nil discarded.provider_job_id
    assert discarded.successfully_enqueued?, "Active Job sets it, so it doesn't mean the call was stored"
    assert_empty events
  end

  test "a bulk enqueue destroyed by a concurrency conflict has no outcome for any of its calls" do
    DiscardingRefreshJob.perform_later("a", "group")
    jobs = 2.times.map { DiscardingRefreshJob.new("b", "group") }

    events = capture_events("coalesce.solid_queue") { assert_equal 0, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ nil, nil ], jobs.map(&:coalesce_status)
    assert_equal [ nil, nil ], jobs.map(&:provider_job_id)
    assert_equal [ false, false ], jobs.map(&:successfully_enqueued?)
    assert_empty events
  end

  test "an enqueue that fails has no outcome and no event" do
    active_job = CoalescedRefreshJob.new("a")
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")

    events = capture_events("coalesce.solid_queue") do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue(active_job) }
    end

    assert_nil active_job.coalesce_status
    assert_empty events
  end

  test "another call with the same key is discarded and reports the open job" do
    first = CoalescedRefreshJob.perform_later("a")
    second = nil

    events = capture_events("coalesce.solid_queue") do
      assert_no_difference -> { SolidQueue::Job.count } do
        second = CoalescedRefreshJob.perform_later("a")

        assert_equal :discarded, second.coalesce_status
        assert second.successfully_enqueued?, "Active Job sets it, so it doesn't mean the call was stored"
        assert_equal first.provider_job_id, second.provider_job_id
        assert_not_equal first.job_id, second.job_id
      end
    end

    assert_equal 1, events.size
    assert_equal "CoalescedRefreshJob/a", events.first.payload[:key]
    assert_equal first.provider_job_id, events.first.payload[:job_id]
    assert_equal :discarded, events.first.payload[:status]
    assert_equal "CoalescedRefreshJob", events.first.payload[:class_name]
    assert_equal second.job_id, events.first.payload[:active_job_id]
  end

  test "enqueueing directly reports a discarded call as not enqueued" do
    CoalescedRefreshJob.perform_later("a")

    discarded = CoalescedRefreshJob.new("a")
    SolidQueue::Job.enqueue(discarded)

    assert_not discarded.successfully_enqueued?
    assert_equal :discarded, discarded.coalesce_status
  end

  test "different keys and different classes are independent" do
    assert_difference -> { SolidQueue::Job.count }, +3 do
      CoalescedRefreshJob.perform_later("a")
      CoalescedRefreshJob.perform_later("b")
      AnotherRefreshJob.perform_later("a")
    end
  end

  test "keys are built the way limits_concurrency builds its key, and stored in the same kind of column" do
    result = JobResult.create!

    [ [ "literal", [] ], [ :literal, [] ], [ ->(name) { name }, [ "a" ] ], [ ->(record) { record }, [ result ] ], [ ->(*) { nil }, [] ] ].each do |key, arguments|
      job_class = Class.new(ApplicationJob) do
        def self.name = "ParityJob"

        coalesce key: key
        limits_concurrency key: key
      end

      assert_equal job_class.new(*arguments).concurrency_key, job_class.new(*arguments).coalesce_key
    end

    coalesce_key = SolidQueue::CoalesceKey.columns_hash["key"]
    semaphore_key = SolidQueue::Semaphore.columns_hash["key"]
    assert_equal [ semaphore_key.sql_type, semaphore_key.null, semaphore_key.collation ], [ coalesce_key.sql_type, coalesce_key.null, coalesce_key.collation ]

    SolidQueue::Record.connection_pool.with_connection do |connection|
      assert connection.index_exists?(:solid_queue_coalesce_keys, :key, unique: true)
    end
  end

  test "a nil or false key declaration disables coalescing, like limits_concurrency" do
    [ nil, false ].each do |key|
      job_class = Class.new(ApplicationJob) do
        def self.name = "ParityJob"

        coalesce key: key
        limits_concurrency key: key
      end

      assert_nil job_class.new.coalesce_key
      assert_nil job_class.new.concurrency_key
      assert_not job_class.new.coalesced_enqueue?
      assert_not job_class.new.concurrency_limited?
    end
  end

  test "keys are built from a record's class and id, or a string" do
    result = JobResult.create!

    active_job = RecordKeyedRefreshJob.perform_later(result)
    assert_equal "SolidQueue::CoalesceSimpleTest::RecordKeyedRefreshJob/JobResult/#{result.id}", active_job.coalesce_key
    assert_equal :discarded, RecordKeyedRefreshJob.perform_later(JobResult.find(result.id)).coalesce_status

    assert_equal "SolidQueue::CoalesceSimpleTest::StringKeyedRefreshJob/everything", StringKeyedRefreshJob.new.coalesce_key
  end

  test "the first schedule wins and a later wait doesn't move the open job" do
    first = CoalescedRefreshJob.set(wait: 1.hour).perform_later("a")
    job = SolidQueue::Job.find(first.provider_job_id)
    scheduled_at = job.scheduled_at

    assert_no_changes -> { job.reload.scheduled_at } do
      assert_equal :discarded, CoalescedRefreshJob.set(wait: 1.minute).perform_later("a").coalesce_status
      assert_equal :discarded, CoalescedRefreshJob.perform_later("a").coalesce_status
    end

    assert job.scheduled?
    assert_equal scheduled_at, job.scheduled_execution.scheduled_at
  end

  test "the key is released when the job starts and a new call opens a new job" do
    CoalescedRefreshJob.perform_later("a")

    assert_equal 1, SolidQueue::CoalesceKey.count
    keys_while_performing = perform_ready_jobs_with_callback { SolidQueue::CoalesceKey.count }

    assert_equal [ 0 ], keys_while_performing
    assert_equal [ "a" ], JobBuffer.values

    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
    assert_equal 1, SolidQueue::CoalesceKey.count
  end

  test "a call while the job is performing opens a new job" do
    CoalescedRefreshJob.perform_later("a")
    joined = nil

    perform_ready_jobs_with_callback { joined = CoalescedRefreshJob.perform_later("a").coalesce_status }

    assert_equal :created, joined
    assert_equal 1, SolidQueue::ReadyExecution.count
  end

  test "there's no time window to configure" do
    assert_raises(ArgumentError) { Class.new(ApplicationJob) { coalesce key: "a", duration: 5.minutes } }
  end

  test "discarding the open job frees the key" do
    active_job = CoalescedRefreshJob.perform_later("a")

    SolidQueue::Job.find(active_job.provider_job_id).discard

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
  end

  test "deleting the open job frees the key" do
    CoalescedRefreshJob.perform_later("a")

    SolidQueue::Job.where(class_name: "CoalescedRefreshJob").delete_all

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
  end

  test "a job discarded by a concurrency conflict on enqueue leaves no key" do
    DiscardingRefreshJob.perform_later("a", "group")
    discarded = DiscardingRefreshJob.perform_later("b", "group")

    assert_nil discarded.coalesce_status
    assert_nil discarded.provider_job_id
    assert_equal [ "SolidQueue::CoalesceSimpleTest::DiscardingRefreshJob/a" ], SolidQueue::CoalesceKey.pluck(:key)
    assert_equal :created, DiscardingRefreshJob.perform_later("b", "another_group").coalesce_status
  end

  test "a scheduled job discarded by a concurrency conflict when it's due frees the key" do
    DiscardingRefreshJob.perform_later("a", "group")
    DiscardingRefreshJob.set(wait: 1.second).perform_later("b", "group")

    travel_to 2.seconds.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    end

    assert_equal :created, DiscardingRefreshJob.perform_later("b", "another_group").coalesce_status
    assert_equal :discarded, DiscardingRefreshJob.perform_later("a", "another_group").coalesce_status
  end

  test "a retry doesn't get suppressed by a pending job or absorb later calls" do
    RetriedRefreshJob.perform_later("a")

    pending = nil
    perform_ready_jobs_with_callback { pending ||= RetriedRefreshJob.perform_later("a") }

    assert_equal :created, pending.coalesce_status
    assert_equal 2, SolidQueue::ReadyExecution.count
    assert_equal pending.provider_job_id, SolidQueue::CoalesceKey.sole.job_id

    # Later calls join the pending job, not the retry
    assert_equal pending.provider_job_id, RetriedRefreshJob.perform_later("a").provider_job_id
  end

  test "a retry after the key is free doesn't take it" do
    RetriedRefreshJob.perform_later("a")

    perform_ready_jobs

    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, RetriedRefreshJob.perform_later("a").coalesce_status
  end

  test "a retry that can't be enqueued fails the run" do
    RetriedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")
    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }
    SolidQueue::Job.unstub(:create!)

    assert job.reload.failed?
    assert_equal SolidQueue::Job::EnqueueError.name, job.failed_execution.exception_class
  end

  test "a retry stopped by a callback fails the run too" do
    AbortedRetryRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert job.reload.failed?
  end

  test "a retry discarded by its own concurrency limit fails the run" do
    RetriedDiscardingRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal SolidQueue::Job::EnqueueError.name, job.failed_execution.exception_class
  end

  test "an ordinary job never touches the coalescing tables when it finishes, fails or gets discarded" do
    [ true, false ].each do |preserve|
      SolidQueue.with(preserve_finished_jobs: preserve) do
        assert_no_coalescing_queries("enqueue") { AddToBufferJob.perform_later("a") }
        assert_no_coalescing_queries("finish, preserve_finished_jobs: #{preserve}") { perform_ready_jobs }
      end
    end

    assert_equal [ "a", "a" ], JobBuffer.values

    OrdinaryFailingJob.perform_later
    assert_no_coalescing_queries("fail") { assert_raises(ExpectedTestError) { perform_ready_jobs } }
    failed = SolidQueue::Job.failed.sole
    assert_no_coalescing_queries("discard a failed job") { failed.discard }

    AddToBufferJob.perform_later("ready")
    assert_no_coalescing_queries("discard a ready job") { SolidQueue::Job.last.discard }

    AddToBufferJob.set(wait: 1.hour).perform_later("scheduled")
    assert_no_coalescing_queries("discard a scheduled job") { SolidQueue::Job.last.discard }
    assert_equal 0, SolidQueue::ScheduledExecution.count
  end

  test "failing an ordinary job opens only Solid Queue's own savepoint" do
    OrdinaryFailingJob.perform_later

    queries = capture_queries { assert_raises(ExpectedTestError) { perform_ready_jobs } }

    assert_equal 1, queries.count { |sql| sql.match?(/\ASAVEPOINT/i) }, queries.to_s
  end

  test "bulk discards read the keys table once per pass, however many jobs are in it" do
    AddToBufferJob.perform_later("a")
    AddToBufferJob.perform_later("b")

    reads = capture_key_queries { SolidQueue::ReadyExecution.discard_all_in_batches }

    assert_equal 1, reads.size, reads.to_s
  end

  test "admission shares the key's lock" do
    skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"

    key_locks = ->(&enqueues) do
      queries = []
      callback = ->(*, payload) { queries << payload[:sql][/FOR UPDATE|FOR SHARE|LOCK IN SHARE MODE/] if payload[:sql].include?("solid_queue_coalesce_keys") }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &enqueues)
      queries.compact.uniq
    end

    assert_includes [ [ "FOR SHARE" ], [ "LOCK IN SHARE MODE" ] ], key_locks.call { 2.times { CoalescedRefreshJob.perform_later("a") } }
  end

  test "a collecting admission locks the key exclusively" do
    skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"

    queries = capture_key_queries { 2.times { CollectingRefreshJob.perform_later("a") } }

    assert_equal [ "FOR UPDATE" ], queries.filter_map { |sql| sql[/FOR UPDATE|FOR SHARE|LOCK IN SHARE MODE/] }.uniq
  end

  test "a collecting admission for a held key locks it without trying to insert it" do
    CollectingRefreshJob.perform_later("a")

    queries = capture_key_queries { CollectingRefreshJob.perform_later("a") }

    assert_empty queries.grep(/\AINSERT/), queries.to_s
  end

  test "a collecting admission for a new key on MySQL inserts it without a locking read first" do
    skip "Gap locks only exist on InnoDB" unless SolidQueue::Record.connection_pool.db_config.adapter.match?(/mysql|trilogy/)

    queries = capture_key_queries { CollectingRefreshJob.perform_later("a") }
    insert = queries.index { |sql| sql.start_with?("INSERT") }

    assert insert, queries.to_s
    assert_empty queries.first(insert).grep(/FOR UPDATE/), queries.to_s
  end

  test "a deferred call reaches the queue when the app's transaction commits" do
    skip_without_enqueue_after_transaction_commit

    active_job = nil
    JobResult.transaction do
      active_job = DeferredRefreshJob.perform_later("a")

      assert_equal 0, SolidQueue::Job.count
      assert_nil active_job.coalesce_status
      assert_nil active_job.provider_job_id
      assert active_job.successfully_enqueued?
    end

    assert_equal :created, active_job.coalesce_status
    assert_equal SolidQueue::Job.sole.id, active_job.provider_job_id
    # Before Rails 8.1, enqueue callbacks run before the commit
    assert_equal [ ActiveJob.version < "8.1" ? nil : :created ], JobBuffer.values
  end

  test "a deferred call is dropped when the app's transaction rolls back" do
    skip_without_enqueue_after_transaction_commit

    active_job = nil
    JobResult.transaction do
      active_job = DeferredRefreshJob.perform_later("a")
      raise ActiveRecord::Rollback
    end

    assert_nil active_job.coalesce_status
    assert_nil active_job.provider_job_id
    assert active_job.successfully_enqueued?
    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a deferred call coalesces on commit, with the job that's pending by then" do
    skip_without_enqueue_after_transaction_commit

    first = DeferredRefreshJob.perform_later("a")
    second = nil
    JobResult.transaction { second = DeferredRefreshJob.perform_later("a") }

    assert_equal :discarded, second.coalesce_status
    assert_equal first.provider_job_id, second.provider_job_id
    assert second.successfully_enqueued?
    assert_equal 1, SolidQueue::Job.count
  end

  test "a failure in the start step becomes a failed execution and perform doesn't run" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole
    SolidQueue::CoalesceKey.stubs(:lock_for_release).raises(ActiveRecord::StatementInvalid, "boom").then.returns(nil)

    assert_raises(ActiveRecord::StatementInvalid) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
  end

  test "a job of a pruned process that hadn't started frees the key when it fails" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "pruned-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id)
    process.update_column(:last_heartbeat_at, 1.day.ago)
    SolidQueue::Process.prune

    assert job.reload.failed?
    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
  end

  test "a job whose class is gone frees the key when it fails" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole
    rename_job_class(job, "RemovedRefreshJob")

    assert_raises(NameError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
  end

  test "a marked job whose class stopped coalescing releases its key when it finishes, so calls can't join a job that won't run them" do
    [ true, false ].each do |preserve|
      SolidQueue.with(preserve_finished_jobs: preserve) do
        StoppedRefreshJob.coalesce key: ->(name) { name }
        StoppedRefreshJob.perform_later("a")
        stop_coalescing(StoppedRefreshJob)

        perform_ready_jobs
        assert_equal 0, SolidQueue::CoalesceKey.count, "preserve_finished_jobs: #{preserve}"

        StoppedRefreshJob.coalesce key: ->(name) { name }
        assert_equal :created, StoppedRefreshJob.perform_later("a").coalesce_status
        perform_ready_jobs
      end
    end

    assert_equal [ "a", "a", "a", "a" ], JobBuffer.values
  ensure
    stop_coalescing(StoppedRefreshJob)
  end

  test "a job that lost its marker still releases its key while its class coalesces" do
    discard = enqueue_unmarked("discard")
    discard.discard
    assert_equal 0, SolidQueue::CoalesceKey.count

    failed = enqueue_unmarked("fail")
    SolidQueue::ReadyExecution.where(job_id: failed.id).delete_all
    failed.failed_with(ExpectedTestError.new("boom"))
    assert failed.reload.failed?
    assert_equal 0, SolidQueue::CoalesceKey.count

    enqueue_unmarked("finish")
    perform_ready_jobs
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal [ "finish" ], JobBuffer.values
  end

  test "finishing a job that was never coalesced leaves other keys alone" do
    CoalescedRefreshJob.set(wait: 1.hour).perform_later("a")
    AddToBufferJob.perform_later("other")

    perform_ready_jobs

    assert_equal 1, SolidQueue::CoalesceKey.count
  end

  test "concurrency limits stay independent and a blocked job holds its key" do
    LimitedRefreshJob.perform_later("a", "group")

    blocked = LimitedRefreshJob.perform_later("b", "group")
    job = SolidQueue::Job.find(blocked.provider_job_id)
    assert job.blocked?
    assert_equal "#{LimitedRefreshJob.name}/group", job.concurrency_key

    # Joining the blocked job doesn't change it, and its concurrency key stays
    # the one computed for the call that created it
    joined = LimitedRefreshJob.perform_later("b", "another_group")
    assert_equal :discarded, joined.coalesce_status
    assert_equal blocked.provider_job_id, joined.provider_job_id
    assert_equal "#{LimitedRefreshJob.name}/group", job.reload.concurrency_key

    perform_ready_jobs
    assert_equal [ "a" ], JobBuffer.values
    assert_equal [ job.id ], SolidQueue::ReadyExecution.pluck(:job_id)

    perform_ready_jobs
    assert_equal [ "a", "b" ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "bulk enqueue with duplicates creates one job per key and counts only what was enqueued" do
    CoalescedRefreshJob.perform_later("existing")

    jobs = [ "a", "b", "a", "existing", "a" ].map { |name| CoalescedRefreshJob.new(name) } + [ AddToBufferJob.new("normal") ]

    assert_difference -> { SolidQueue::Job.count }, +3 do
      assert_equal 3, enqueued_count_for(jobs)
    end

    assert_equal [ true, true, false, false, false, true ], jobs.map(&:successfully_enqueued?)
    assert_equal [ :created, :created, :discarded, :discarded, :discarded, nil ], jobs.map(&:coalesce_status)
    assert_equal jobs[0].provider_job_id, jobs[2].provider_job_id
    assert_equal jobs[0].provider_job_id, jobs[4].provider_job_id
    assert jobs[5].provider_job_id.present?
  end

  test "a duplicate nested in a transaction on the same database doesn't abort it" do
    CoalescedRefreshJob.perform_later("a")

    SolidQueue::Record.transaction do
      assert SolidQueue::Job.enqueue(CoalescedRefreshJob.new("c")).present?
      SolidQueue::Job.enqueue(CoalescedRefreshJob.new("a"))
      SolidQueue::Job.enqueue_all([ CoalescedRefreshJob.new("a"), CoalescedRefreshJob.new("c") ])

      assert_equal 2, SolidQueue::Job.count
    end

    assert_equal 2, SolidQueue::Job.count
    assert_equal 2, SolidQueue::CoalesceKey.count
  end

  test "rolling back the outer transaction leaves no key behind" do
    SolidQueue::Record.transaction do
      SolidQueue::Job.enqueue(CoalescedRefreshJob.new("a"))
      raise ActiveRecord::Rollback
    end

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
  end

  private
    def enqueue_unmarked(name)
      job = SolidQueue::Job.find(CoalescedRefreshJob.perform_later(name).provider_job_id)
      job.update!(arguments: job.arguments.except("solid_queue_coalesce_on_conflict"))
      SolidQueue::Job.find(job.id)
    end

    def assert_no_coalescing_queries(message, &block)
      queries = capture_queries(&block).grep(/solid_queue_coalesce/)
      assert_empty queries, message
    end

    def capture_queries(&block)
      queries = []
      callback = ->(*, payload) { queries << payload[:sql] unless payload[:name] == "SCHEMA" }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
      queries
    end

    def capture_key_queries(&block)
      queries = []
      callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].include?("solid_queue_coalesce_keys") && payload[:name] != "SCHEMA" }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
      queries
    end

    def skip_without_enqueue_after_transaction_commit
      skip "Needs Rails 7.2" unless ActiveJob::Base.respond_to?(:enqueue_after_transaction_commit=)
    end

    def enqueued_count_for(jobs)
      SolidQueue::Job.enqueue_all(jobs)
    end

    def capture_events(name)
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(name) { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
end
